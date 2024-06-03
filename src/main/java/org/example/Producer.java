package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
        log.info("Starting message Producer");

        Properties properties = getProperties();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            //nerds
            HashMap<String, String> characters = new HashMap<String, String>();
            characters.put("hobbits", "Frodo");
            characters.put("hobbits", "Sam");
            characters.put("elves", "Galadriel");
            characters.put("elves", "Arwen");
            characters.put("humans", "Éowyn");
            characters.put("humans", "Faramir");

            for (HashMap.Entry<String, String> character : characters.entrySet()) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("lotr_characters", character.getKey(), character.getValue());
                //oooh gettin fancy with lambda.
                producer.send(producerRecord, (RecordMetadata recordMetadata, Exception err) -> {
                    if (err == null) {
                        log.info("Message received. \ntopic [{}]\npartition [{}]\noffset [{}]\ntimestamp [{}]", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        log.error("An error occurred while producing messages", err);
                    }
                });
            }
            //lol no producer.close() thanks try-with-resources.
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //I guess we could have arbitrary data as the key. I'd guess we end up using hashCode to provide fast lookup
        // over the events.
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
