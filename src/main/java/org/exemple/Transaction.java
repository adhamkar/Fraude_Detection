package org.exemple;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Transaction {
    private String userId;
    private double amount;
    private String timestamp;

    public Transaction() {
    }

    public Transaction(String userId, double amount, String timestamp) {
        this.userId = userId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "userId='" + userId + '\'' +
                ", amount=" + amount +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
    @JsonProperty("userId")
    public String getUserId() {
        return userId;
    }
    @JsonProperty("amount")
    public double getAmount() {
        return amount;
    }
    @JsonProperty("timestamp")
    public String getTimestamp() {
        return timestamp;
    }
    public static Transaction fromJson(String json) throws Exception{
        ObjectMapper objectMapper=new ObjectMapper();
        return objectMapper.readValue(json, Transaction.class);
    }
}
