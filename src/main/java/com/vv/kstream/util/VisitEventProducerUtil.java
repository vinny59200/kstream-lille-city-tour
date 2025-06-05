package com.vv.kstream.util;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class VisitEventProducerUtil {

    /**
     * Reads CSV lines from a resource file in the classpath.
     * @param resourcePath path to the CSV file in resources folder, e.g. "/visit-events.csv"
     * @return list of lines from the CSV
     */
    public static List<String> readEventsFromResource(String resourcePath) {
        List<String> events = new ArrayList<>();
        try (InputStream is = VisitEventProducerUtil.class.getResourceAsStream(resourcePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    events.add(line.trim());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to read CSV resource: " + resourcePath, e);
        }
        return events;
    }

    /**
     * Sends the list of events to the specified Kafka topic.
     * @param bootstrapServers Kafka bootstrap servers
     * @param topic Kafka topic to send messages to
     * @param events List of CSV event lines to send
     */
    public static void produceEvents(String bootstrapServers, String topic, List<String> events) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String event : events) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, event);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Failed to send event: " + event);
                        exception.printStackTrace();
                    } else {
                        System.out.printf("Sent: %s to partition %d offset %d%n", event, metadata.partition(), metadata.offset());
                    }
                });
            }
            producer.flush();
        }
    }
}
