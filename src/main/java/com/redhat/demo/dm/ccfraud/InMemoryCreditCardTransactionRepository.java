package com.redhat.demo.dm.ccfraud;

import com.redhat.demo.dm.ccfraud.domain.CreditCardTransaction;

import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Simple {@link CreditCardTransactionRepository} implementation that stores the credit-card transactions in memory.
 * 
 * @author <a href="mailto:duncan.doyle@redhat.com">Duncan Doyle</a>
 */
public class InMemoryCreditCardTransactionRepository implements CreditCardTransactionRepository {

	private static final String TRANSACTIONS_CSV_FILE = "ccTransactions.csv";
	
	private Map<Long, List<CreditCardTransaction>> ccTransactions;
	
	public InMemoryCreditCardTransactionRepository() {
		ccTransactions = new HashMap<>();
		
		//Load the facts/events from our CSV file.
		InputStream ccTransactionsInputStream = App.class.getClassLoader().getResourceAsStream(TRANSACTIONS_CSV_FILE);
		List<CreditCardTransaction> loadedTransactions = FactsLoader.loadFacts(ccTransactionsInputStream);
				
		for (CreditCardTransaction nextTransaction: loadedTransactions) {
			List<CreditCardTransaction> cardTransactions = ccTransactions.get(nextTransaction.getCreditCardNumber());
			
			if (cardTransactions == null) {
				cardTransactions = new ArrayList<>();
				ccTransactions.put(nextTransaction.getCreditCardNumber(), cardTransactions);
			}
			
			cardTransactions.add(nextTransaction);
		}	
	}
	
	@Override
	public Collection<CreditCardTransaction> getCreditCardTransactionsForCC(long creditCardNumber, long timestamp) {
		return ccTransactions.get(creditCardNumber).stream().filter(cct -> (cct.getTimestamp() >= timestamp)).collect(Collectors.toList());
	}

	@Override
	public Collection<CreditCardTransaction> getCreditCardTransactionsForCC(long creditCardNumber) {
		return ccTransactions.get(creditCardNumber);
	}

}
