package com.redhat.demo.dm.ccfraud.domain;

import java.util.List;

public class PotentialFraudFact {

    private Long creditCardNumber;
    private List<CreditCardTransaction> transactions;

    public Long getCreditCardNumber() {
        return creditCardNumber;
    }

    public void setCreditCardNumber(Long creditCardNumber) {
        this.creditCardNumber = creditCardNumber;
    }

    public List<CreditCardTransaction> getTransactions() {
        return transactions;
    }

    public void setTransactions(List<CreditCardTransaction> transactions) {
        this.transactions = transactions;
    }

    public PotentialFraudFact(Long creditCardNumber, List<CreditCardTransaction> transactions) {
        this.creditCardNumber = creditCardNumber;
        this.transactions = transactions;
    }

    @Override
    public String toString() {
        return "PotentialFraudFact{" +
                "creditCardNumber=" + creditCardNumber +
                ", transactions=" + transactions +
                '}';
    }
}
