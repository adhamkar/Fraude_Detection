package org.exemple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Timestamp;
import java.util.Properties;

public class TransactionProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("application.id","fraud-TP");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> transactionProducer = new KafkaProducer<>(props);
        long offset = Timestamp.valueOf("2023-01-01 00:00:00").getTime();
        long end = Timestamp.valueOf("2024-01-01 00:00:00").getTime();
        long diff = end - offset + 1;
        //100 transaction to send
        // Infinite loop to generate continuous transactions
        while (true) {
            Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));
            String key = "user" + (int)(Math.random() * 100 + 1); // Randomize user
            String value = "{\"userId\":\"" + key + "\", \"amount\": " + (int)(Math.random() * 20000) + ", \"timestamp\": \""+rand+"\"}";
            ProducerRecord<String, String> record = new ProducerRecord<>("transactions-input", key, value);
            transactionProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending message: " + exception.getMessage());
                } else {
                    System.out.printf("Message sent successfully to partition %d, offset %d%n",
                            metadata.partition(), metadata.offset());
                    System.out.println("value: "+record.value());
                }
            });
            Thread.sleep(1000); // 1-second delay for readability (optional)
        }

    }
}
