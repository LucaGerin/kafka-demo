package org.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/*
 * Exactly Once Semantics (EOS) significa che ogni messaggio prodotto viene scritto nel topic
 * una e una sola volta, anche in caso di errori o ritentativi.
 *
 * Alternative:
 * - At most once: i messaggi possono andare persi, ma non duplicati.
 * - At least once: i messaggi non vengono persi, ma possono essere duplicati.
 *
 * Un producer Kafka normale (non EOS) è per default "at least once": ritenta in caso di errore,
 * ma può produrre duplicati se non usi idempotenza o transazioni.
 *
 * Questo producer invece implementa EOS
 * NB: Per un producer che è anche un consumer, è necessario anche utilizzare:
 * producer.sendOffsetsToTransaction(offsetsMap, consumerGroupId);
 * che in questa classe non è utilizzato, non essendo il caso.
 */

public class KafkaProducerEOSDemo {

    private final static String TOPIC = "demo-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    // TEST: per simulare un numero casuale di errori mettere TRUE
    private final static Boolean someErrorsBeforeCommit = true;

    public static void runTransactionalProducer() {

        // Usato dopo per simulare errore un numero di volte
        Random random = new Random();

        // === Configurazione base del Kafka Producer ===
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // === Configurazioni per Exactly-Once Semantics ===
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");           // Idempotenza attiva (obbligatoria per EOS, già da sola garantisce un buon livello di EOS)
        props.put(ProducerConfig.ACKS_CONFIG, "all");                           // Garantisce che tutte le ISR confermino

        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)); // Infiniti tentativi
        // NB: il retry automatico funziona solo per errori transitori interni a Kafka, non per l’eccezione simulata manualmente

        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");  // <=5 per EOS (1-5 OK da Kafka 2.5+)

        // ID univoco per transazioni,Permette al broker Kafka di gestire fencing, commit/abort e l’isolamento delle transazioni.
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "eos-producer-1");
        // NB: In ambienti distribuiti o con failover, è importante che ogni istanza del producer abbia un transactional ID univoco.


        // === Timeout legati a invio ===
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

                    // TEST: Simula un errore inatteso prima del commit
                    if(someErrorsBeforeCommit){
                        if(random.nextInt(3) == 0){ // Questo blocco viene eseguito circa 1/3 delle volte
                            System.out.println("[EOS Producer]: Simulo un errore durante l'invio del messaggio " + i + ", prima del commit");
                            throw new RuntimeException("Errore simulato prima del commit!");
                        }
                    }

                    // Commit della transazione se tutto è andato a buon fine
                    producer.commitTransaction();

                } catch (ProducerFencedException e) {
                    // Il producer è stato "fenced" (escluso) da Kafka perché un altro producer con lo stesso transactional.id è attivo
                    System.err.println("[EOS Producer]: ProducerFencedException: questo producer non è più valido. Chiudo.");
                    producer.close(); // Chiusura obbligatoria: producer non più utilizzabile
                } catch (Exception e) {
                    // Altri errori imprevisti durante la transazione (e.g. problemi di rete, serializzazione, etc.)
                    System.err.println("[EOS Producer]: ❌ Errore durante la transazione, eseguo rollback.");
                    producer.abortTransaction(); // Annulla la transazione corrente
                }


                Thread.sleep(500); // Simula intervallo tra i messaggi
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("[EOS Producer]: ✅ Completato l'invio dei messaggi con Exactly-Once Semantic policy.");
    }

}
