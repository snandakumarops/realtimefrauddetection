package com.redhat.demo.dm.ccfraud;


import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.redhat.demo.dm.ccfraud.domain.CountryCode;
import com.redhat.demo.dm.ccfraud.domain.CreditCardTransaction;
import com.redhat.demo.dm.ccfraud.domain.PotentialFraudFact;
import com.redhat.demo.dm.ccfraud.domain.Terminal;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.StatelessKieSession;
import org.kie.api.runtime.rule.EntryPoint;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.time.SessionClock;
import org.kie.api.time.SessionPseudoClock;
import org.kie.internal.command.CommandFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {


    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd:HHmmssSSS");

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    private static KieContainer kieContainer;

    private static CreditCardTransactionRepository cctRepository = new InMemoryCreditCardTransactionRepository();
    public static void main(String[] args) throws Exception {
        String brokers = "localhost:9092";
        String intopic = "events";
        String outtopic = "output";

        if (brokers == null) {
            System.out.println("KAFKA_BROKERS must be defined.");
            System.exit(1);
        }
        if (intopic == null) {
            System.out.println("KAFKA_IN_TOPIC must be defined.");
            System.exit(1);
        }

        if (outtopic == null) {
            System.out.println("KAFKA_OUT_TOPIC must be defined.");
            System.exit(1);
        }

        /* setup the schema for our messages */
        StructType event_msg_struct = new StructType()
                .add("transactionNumber", DataTypes.StringType)
                .add("creditCardNumber", DataTypes.StringType)
                .add("amount", DataTypes.StringType)
                .add("timestamp", DataTypes.StringType)
                .add("terminal", DataTypes.StringType);


        /* acquire a SparkSession object */
        SparkSession spark = SparkSession
                .builder()
                .appName("KafkaSparkOpenShiftJava")
                .getOrCreate();

        /* setup rules processing */
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        KieBase rules = loadRules();
        Broadcast<KieBase> broadcastRules = sc.broadcast(rules);

        /* register a user defined function to apply rules on events */
        spark.udf().register("eventfunc", (String transactionNumber, String creditCardNumber,
                                           String amount, String timestamp, String terminal) -> {
            StatelessKieSession session = broadcastRules.value().newStatelessKieSession();
            /* create a random "confidence" score to be used by some rules */
            Random rnd = new Random();
            int cnf = rnd.nextInt(101); /* want a range between 0-100 */
            CreditCardTransaction e = new
                    CreditCardTransaction(new Long(transactionNumber),new Long(creditCardNumber),
                    new BigDecimal(amount),new Long(timestamp),new Terminal(1L,CountryCode.valueOf(terminal)));
            String jsonResponse = processTransaction(e);
            return jsonResponse;

        }, DataTypes.StringType);

        /* configure the operations to read the input topic */
        Dataset<Row> records = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("subscribe", intopic)
                .load()
                .select(functions.column("value").cast(DataTypes.StringType).alias("value"))
                .select(functions.from_json(functions.column("value"), event_msg_struct).alias("json"))
                .select(functions.callUDF("eventfunc",
                        functions.column("json.transactionNumber"),
                        functions.column("json.creditCardNumber"),
                        functions.column("json.amount"),
                        functions.column("json.timestamp"),
                        functions.column("json.terminal")
                        ).alias("value"))
                .select(functions.to_json(functions.column("value")).alias("value"));



        List<Row> potentialFraud = records.collectAsList();
        PotentialFraudFact potentialFraudFact = null;

        for(Row row:potentialFraud) {
            String jsonString = row.getString(1);
            potentialFraudFact = new Gson().fromJson(jsonString,PotentialFraudFact.class);
            System.out.print("PotentialFraudFact"+potentialFraudFact);
//            CaseMgmt caseMgmt = new CaseMgmt();
//            caseMgmt.invokeCase(potentialFraudFact);
        }
    }

    public static KieBase loadRules() {
        KieServices kieServices = KieServices.Factory.get();
        KieContainer kieContainer = kieServices.getKieClasspathContainer();
        return kieContainer.getKieBase();
    }
    private static String processTransaction(CreditCardTransaction ccTransaction) {
        // Retrieve all transactions for this account
        Collection<CreditCardTransaction> ccTransactions = cctRepository
                .getCreditCardTransactionsForCC(ccTransaction.getCreditCardNumber());
        PotentialFraudFact potentialFraudFact = null;

        if(ccTransactions == null) {
            return null;
        }
        System.out.println("Found '" + ccTransactions.size() + "' transactions for creditcard: '" + ccTransaction.getCreditCardNumber() + "'.");

        KieSession kieSession = kieContainer.newKieSession();
        // Insert transaction history/context.
        System.out.println("Inserting credit-card transaction context into session.");
        for (CreditCardTransaction nextTransaction : ccTransactions) {
            insert(kieSession, "Transactions", nextTransaction);
        }
        // Insert the new transaction event
        System.out.println("Inserting credit-card transaction event into session.");
        insert(kieSession, "Transactions", ccTransaction);
        // And fire the com.redhat.demo.dm.com.redhat.demo.dm.ccfraud.rules.
        kieSession.fireAllRules();

        Collection<?> fraudResponse = kieSession.getObjects();




        // Dispose the session to free up the resources.
        kieSession.dispose();
        return new Gson().toJson(potentialFraudFact);

    }

    /**
     * CEP insert method that inserts the event into the Drools CEP session and programatically advances the session clock to the time of
     * the current event.
     *
     * @param kieSession
     *            the session in which to insert the event.
     * @param stream
     *            the name of the Drools entry-point in which to insert the event.
     * @param cct
     *            the event to insert.
     *
     * @return the {@link FactHandle} of the inserted fact.
     */
    private static FactHandle insert(KieSession kieSession, String stream, CreditCardTransaction cct) {
        SessionClock clock = kieSession.getSessionClock();
        if (!(clock instanceof SessionPseudoClock)) {
            String errorMessage = "This fact inserter can only be used with KieSessions that use a SessionPseudoClock";
            LOGGER.error(errorMessage);
            throw new IllegalStateException(errorMessage);
        }
        SessionPseudoClock pseudoClock = (SessionPseudoClock) clock;
        EntryPoint ep = kieSession.getEntryPoint(stream);

        // First insert the event
        FactHandle factHandle = ep.insert(cct);
        // And then advance the clock.

        long advanceTime = cct.getTimestamp() - pseudoClock.getCurrentTime();
        if (advanceTime > 0) {
            System.out.println("Advancing the PseudoClock with " + advanceTime + " milliseconds.");
            pseudoClock.advanceTime(advanceTime, TimeUnit.MILLISECONDS);
        } else {
            // Print a warning when we don't need to advance the clock. This usually means that the events are entering the system in the
            // incorrect order.
            LOGGER.warn("Not advancing time. CreditCardTransaction timestamp is '" + cct.getTimestamp() + "', PseudoClock timestamp is '"
                    + pseudoClock.getCurrentTime() + "'.");
        }
        return factHandle;
    }
}