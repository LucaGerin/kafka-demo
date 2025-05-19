package org.example.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaTopicInitializer {
    private static final String BOOTSTRAP_SERVERS = "kafka1:9092"; // usa il nome container, NON localhost
    private static final String TOPIC_NAME = "demo-topic";

    public static void createTopicIfNotExists() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {

            waitForKafka(adminClient, Duration.ofSeconds(30));

            Set<String> topicNames = adminClient.listTopics().names().get();
            if (!topicNames.contains(TOPIC_NAME)) {
                NewTopic newTopic = new NewTopic(TOPIC_NAME, 3, (short) 1);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                System.out.println("✅ Topic creato: " + TOPIC_NAME);
            } else {
                System.out.println("ℹ️  Il topic: " + TOPIC_NAME + " esiste.");
            }

        } catch (Exception e) {
            System.err.println("❌ Errore nella creazione/verifica del topic:");
            e.printStackTrace();
        }
    }

    private static void waitForKafka(AdminClient adminClient, Duration timeout) {
        Instant start = Instant.now();
        while (true) {
            try {
                adminClient.listTopics().names().get();
                System.out.println("🟢 Kafka è pronto!");
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrotto durante l'attesa di Kafka", e);
            } catch (ExecutionException e) {
                if (Duration.between(start, Instant.now()).compareTo(timeout) > 0) {
                    throw new RuntimeException("❌ Timeout: Kafka non è pronto dopo " + timeout.getSeconds() + " secondi", e);
                }
                System.out.println("⏳ Kafka non è ancora pronto. Riprovo tra 3 secondi...");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrotto durante l'attesa di Kafka", ex);
                }
            }
        }
    }
}

