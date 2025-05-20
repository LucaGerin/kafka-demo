package org.example.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaTopicInitializer {

    // Indirizzo del broker Kafka (usare il nome del container quando si esegue dentro Docker)
    private static final String BOOTSTRAP_SERVERS = "kafka1:9092";

    // Nome del topic da verificare o creare
    private static final String TOPIC_NAME = "demo-topic";

    // Numero di partizioni del topic
    private static final int numPartitions = 3;

    // Metodo che verifica se il topic esiste, altrimenti lo crea
    public static void createTopicIfNotExists() {

        // ---------- Configurazione properties per l'AdminClient Kafka ----------
        Properties props = new Properties();

        // Indica l'indirizzo del broker Kafka a cui connettersi per usare l'AdminClient
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // Prova a connettersi all'AdminClient e gestirlo automaticamente con try-with-resources
        try (AdminClient adminClient = AdminClient.create(props)) {

            // Attende che Kafka sia disponibile, fino a un timeout massimo
            waitForKafka(adminClient, Duration.ofSeconds(30));

            // Ottiene la lista dei topic esistenti nel cluster
            Set<String> topicNames = adminClient.listTopics().names().get();
            System.out.println("📋 Topic presenti nel cluster: " + topicNames);

            // Se il topic desiderato non esiste, procedi alla creazione
            if (!topicNames.contains(TOPIC_NAME)) {

                // Recupera la lista dei broker attualmente attivi nel cluster
                Collection<Node> brokerNodes = adminClient.describeCluster().nodes().get(); //Ogni Node contiene informazioni come: id(). host(), port() ,isEmpty()
                int brokerCount = brokerNodes.size();
                System.out.println("📡 Broker attivi nel cluster: " + brokerCount);

                // Imposta dinamicamente il replication factor in base a i broker disponibili (max 3)
                short replicationFactor = (short) Math.min(3, brokerCount);

                // Se non ci sono broker disponibili, lancia un errore
                if (replicationFactor < 1) {
                    throw new IllegalStateException("❌ Nessun broker disponibile per creare il topic.");
                }

                // Stampa configurazione di creazione prima di procedere
                System.out.printf("📦 Creazione del topic \"%s\" con %d partizioni e replication factor %d%n",
                        TOPIC_NAME, numPartitions, replicationFactor);

                // Prepara le configurazioni personalizzate del topic
                Map<String, String> topicConfigs = new HashMap<>();
                // La configurazione "min.insync.replicas" indica il numero minimo di repliche che devono essere "in-sync"
                // (cioè aggiornate con il leader) affinché Kafka consideri valido un write quando il producer usa "acks=all".
                // Se il numero di repliche in-sync scende al di sotto di questo valore, Kafka rifiuta le scritture con errore
                // per garantire la durabilità dei dati (solo se il producer ha configurato acks=all).
                // Nota: se il producer usa acks=1 o acks=0, questa configurazione NON ha effetto.
                // Nota: avere un alto min.insync.replicas, vicino o uguale al replication factor delle partizioni, significa bassa availability
                int desiredMinInSyncReplicas = Math.max(1, replicationFactor - 1);
                topicConfigs.put("min.insync.replicas", String.valueOf(desiredMinInSyncReplicas));

                System.out.println("🔧 Configurazioni del topic:");
                topicConfigs.forEach((key, value) -> System.out.println(" - " + key + " = " + value));

                // Crea il topic con 3 partizioni e replication factor calcolato e le configs stabilite
                NewTopic newTopic = new NewTopic(TOPIC_NAME, numPartitions, replicationFactor).configs(topicConfigs);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                System.out.println("✅ Topic creato: " + TOPIC_NAME);

            } else {
                // Il topic esiste già, nessuna azione necessaria
                System.out.println("ℹ️ Ho verificato che il topic: " + TOPIC_NAME + " esiste.");
            }

        } catch (Exception e) {
            // Gestione di eventuali errori durante la creazione/verifica
            System.err.println("❌ Errore nella creazione/verifica del topic:");
            e.printStackTrace();
        }
    }


    // Metodo che ttende che Kafka sia pronto a rispondere alle chiamate AdminClient, fino al timeout indicato
    private static void waitForKafka(AdminClient adminClient, Duration timeout) {
        Instant start = Instant.now();
        while (true) {
            try {
                // Prova a ottenere la lista dei topic (come test di connessione)
                adminClient.listTopics().names().get();
                System.out.println("🟢 Kafka è pronto!");
                return; // Kafka è pronto → esce dal metodo
            } catch (InterruptedException e) {
                // Interruzione del thread → propaga come RuntimeException
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrotto durante l'attesa di Kafka", e);
            } catch (ExecutionException e) {
                // Se Kafka non è ancora pronto...
                if (Duration.between(start, Instant.now()).compareTo(timeout) > 0) {
                    // ...e il tempo è scaduto → solleva eccezione
                    throw new RuntimeException("❌ Timeout: Kafka non è pronto dopo " + timeout.getSeconds() + " secondi", e);
                }
                // ...altrimenti aspetta e riprova dopo 3 secondi
                System.out.printf("[%s] ⏳ Kafka non è ancora pronto. Riprovo tra 3 secondi...%n",
                        java.time.LocalTime.now());
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
