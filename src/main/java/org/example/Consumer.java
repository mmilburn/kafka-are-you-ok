package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {
        log.info("Consumer started");
        Properties properties = getProperties();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            // subscribe to topic(s)
            String topic = "lotr_characters";
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                // poll for new messages
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(100));

                // handle message contents
                for (ConsumerRecord<String, String> message : messages) {
                    log.info("key [{}] value [{}]", message.key(), message.value());
                    log.info("partition [{}] offset [{}]", message.partition(), message.offset());
                }
            }
        } catch (Exception e) {
            log.error("Error while consuming messages", e);
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "lotr_consumer_group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
