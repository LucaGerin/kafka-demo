package org.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {

    private final static String TOPIC = "demo-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void runProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 10; i++) {
                String value = "Messaggio numero " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, Integer.toString(i), value);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Messaggio inviato a %s:%d offset=%d%n",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });
                Thread.sleep(500); // Simula tempo tra i messaggi
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
