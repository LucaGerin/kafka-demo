package org.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerDemo {

    private final static String TOPIC = "demo-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String GROUP_ID = "demo-group";

    public static void runConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            System.out.println("✅ Consumer avviato. In attesa di messaggi...");

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("⬇️  Ricevuto: key=%s, value=%s, offset=%d, partition=%d%n",
                            record.key(), record.value(), record.offset(), record.partition());
                }
            }
        } catch (WakeupException e) {
            // Eccezione normale usata per svegliare il consumer quando viene chiuso
            System.out.println("⚠️ Consumer svegliato per chiusura.");
        } catch (InterruptException e) {
            System.out.println("----------------");
            System.out.println("ℹ️  Consumer interrotto.");
        } catch (Exception e) {
            System.err.println("❌ Errore nel consumer:");
            e.printStackTrace();
        } finally {
            System.out.println("🔚 Consumer chiuso.");
        }
    }
}
