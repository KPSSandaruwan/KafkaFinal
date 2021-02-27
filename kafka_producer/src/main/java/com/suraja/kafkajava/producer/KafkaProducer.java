package com.suraja.kafkajava.producer;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducer {
    Logger logger = LoggerFactory.getLogger(KafkaProducer.class.getName());
    public static String itemString = null;

    public KafkaProducer() {}

    public static void main(String[] args) {

        new KafkaProducer().run();
    }

    public void run() {
        logger.info("Setup");



        // Get the correct input product from the user an api call
//        String itemType = "HP i5";
//        String itemType = "iphone7";
        String itemType = "Dell i5";


        // Connect to mongo db
        MongoClientURI uri = new MongoClientURI(
                "mongodb+srv://sandaruwan:suraja2228482@cluster0.kbhc2.mongodb.net/feedbackdb?retryWrites=true&w=majority"
        );

        // Mongo client
        MongoClient mongoClient = new MongoClient(uri);
        MongoDatabase database = mongoClient.getDatabase("feedbackdb");
        MongoCollection<Document> collection = database.getCollection("test");


        // Find One document
        Document item = collection.find(new Document("item_type", itemType)).first();
        assert item != null;
//        System.out.println("HP i7: " + item.toJson());

        itemString = item.toJson();

        // Create Kafka producer
        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = createKafkaProducer();



        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));

//            while (true) {
//                if (s != null) {
//                    try {
//                        ProducerRecord<String, String> record = new ProducerRecord<String, String>("new-feedback2", s);
//                        producer.send(record);
////                        s = null;
//                        System.out.println(record.value());
//                        Thread.sleep(100);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                        break;
//                    }
//                }
//                break;
//            }

        while (true) {
            if (itemString != null) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("new-feedback2", itemString);
                producer.send(record);
                itemString = null;
                System.out.println(record.value());
            } else {
                break;
            }
        }
    }

    public org.apache.kafka.clients.producer.KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServer = "127.0.0.1:9092";


        // Creating properties
        Properties properties = new Properties();

        // Kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe producer
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        // Leverage idempodent producer from kafka
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");


        //Creating a producer
        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);
        return producer;
    }
}
