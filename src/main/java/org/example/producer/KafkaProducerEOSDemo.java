package org.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerEOSDemo {

    private final static String TOPIC = "demo-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void runTransactionalProducer() {

        // Configurazione del Kafka Producer con supporto a Exactly-Once
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // === Configurazioni per Exactly-Once Semantics ===
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");           // Idempotenza attiva (obbligatoria)
        props.put(ProducerConfig.ACKS_CONFIG, "all");                           // Garantisce che tutte le ISR confermino
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)); // Infiniti tentativi
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");  // <=5 per EOS (1-5 OK da Kafka 2.5+)
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "eos-producer-1");   // ID univoco per transazioni

        // Timeout legati a invio
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // Inizializza il producer per le transazioni
            producer.initTransactions();

            for (int i = 1; i <= 10; i++) {
                try {
                    // Avvia una transazione Kafka
                    producer.beginTransaction();

                    String key = "trx-" + i;
                    String value = "Messaggio transazionale numero " + i;
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

                    producer.send(record, (metadata, exception) -> {
                        if (exception == null) {
                            System.out.printf("[EOS Producer]: ✅ Messaggio \"%s\" inviato a topic %s, partizione %d, offset=%d%n",
                                    key, metadata.topic(), metadata.partition(), metadata.offset());
                        } else {
                            System.err.println("[EOS Producer]: ❌ Errore nell'invio del messaggio:");
                            exception.printStackTrace();
                        }
                    });

                    // Commit della transazione se tutto è andato a buon fine
                    producer.commitTransaction();

                } catch (Exception e) {
                    System.err.println("[EOS Producer]: ⚠️ Errore, eseguo rollback della transazione.");
                    producer.abortTransaction();
                }

                Thread.sleep(500); // Simula intervallo tra i messaggi
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("[EOS Producer]: ✅ Completato l'invio dei messaggi con Exactly-Once Semantic policy.");
    }

}
