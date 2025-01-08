package org.exemple;



import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;


import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FraudDetectionApp {
    private static final String TOPIC="transactions-input";
    private static final String FRAUD_TOPIC="fraud-alerts";
    private static final String INFLUXDB_BUCKET = "fraud_detection_db";
    private static final String INFLUXDB_ORG = "enset-org";
    private static final String INFLUXDB_URL = "http://localhost:8086";
    private static final String INFLUXDB_TOKEN="MmgV4_oAkTJHbe2mJ9Cn6-pJzL9nhoMCHfPCEVQ_IDjHNBv01CgUCSzNLg-7yuTRqRqnFBQiIDQYmOT1rgdrQA==";

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("application.id","fraud-detection");
        prop.setProperty("bootstrap.servers", "localhost:9092");
        prop.put("default.key.serde", Serdes.String().getClass());
        prop.put("default.value.serde", Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        //configurer la connexion InfluxDb

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create(
                INFLUXDB_URL,
                INFLUXDB_TOKEN.toCharArray(),
                INFLUXDB_ORG,
                INFLUXDB_BUCKET);
        // Blocking API for writing data
        WriteApi writeApi = influxDBClient.getWriteApi();

        KStream<String, String> transactionsStream = builder.stream(TOPIC);
        //detect fraud transaction
        KStream<String ,String> fraudTransaction=transactionsStream.filter((k,v)->{
            try{
                Transaction transaction=Transaction.fromJson(v);
                return transaction.getAmount()>10000;
            }catch (Exception e){
                e.printStackTrace();
                return false;
            }
        });
        // envoie fes frauds aux topic fraud-alert
        fraudTransaction.to(FRAUD_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // stocker les données dans InfluxDb
        fraudTransaction.foreach((k,v)->{
            try {
                Transaction transaction=Transaction.fromJson(v);
                 Point point= Point.measurement("fraud_transactions")
                                .time(Instant.now(), WritePrecision.MS)
                                .addTag("userId", transaction.getUserId())
                                .addField("amount", transaction.getAmount())
                                .addField("timestamp", transaction.getTimestamp());
                System.out.println("Writing point to InfluxDB: " + point.toString());
                 writeApi.writePoint(point);

            }catch (Exception e){
                System.err.println("Error writing to InfluxDB: " + e.getMessage());
                e.printStackTrace();
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), prop);

        // Add shutdown hook to handle Ctrl+C gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Start Kafka Streams
        streams.start();
    }
}
