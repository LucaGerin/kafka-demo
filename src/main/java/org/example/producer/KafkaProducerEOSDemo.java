package org.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.util.TimestampFormatter;

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

public class KafkaProducerEOSDemo implements MessageProducer<String, String>{


    private final String producerId;
    private final String topic;
    private final Producer<String, String> producer;
    private final boolean simulateErrors; // TEST: per simulare un numero casuale di errori mettere TRUE

    // Codici ANSI per il colore
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLUE = "\u001B[34m";

    public KafkaProducerEOSDemo(String topic, String bootstrapServers, String transactionalId, String producerId, boolean simulateErrors) {
        this.topic = topic;
        this.producerId = producerId;
        this.simulateErrors = simulateErrors;

        // === Configurazione base del Kafka Producer ===
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // === Configurazioni per Exactly-Once Semantics ===
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");    // Idempotenza attiva (obbligatoria per EOS, già da sola garantisce un buon livello di EOS)
        props.put(ProducerConfig.ACKS_CONFIG, "all");                   // Garantisce che tutte le ISR confermino

        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));  // Infiniti tentativi
        // NB: il retry automatico funziona solo per errori transitori interni a Kafka, non per l’eccezione simulata manualmente

        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");   // <=5 per EOS (1-5 OK da Kafka 2.5+)

        // ID univoco per transazioni,Permette al broker Kafka di gestire fencing, commit/abort e l’isolamento delle transazioni.
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        // NB: In ambienti distribuiti o con failover, è importante che ogni istanza del producer abbia un transactional ID univoco.

        // === Timeout legati a invio ===
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");

        // Creare il producer
        this.producer = new KafkaProducer<>(props);

        // Inizializza il producer per le transazioni
        this.producer.initTransactions();
    }

    public String getProducerId() {
        return producerId;
    }

    @Override
    public void sendMessage(String key, String value) {

        // Usato dopo per simulare errore un numero di volte
        Random random = new Random();

        try {
            // Avvia una transazione Kafka
            producer.beginTransaction();

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    String log = String.format(
                            ANSI_BLUE + "[Producer %s]" + ANSI_RESET + ": ✅ Messaggio con key=\"%s\", value=\"%s\", timestamp=%s \n\t\t\t\tinviato al topic \"%s\", partizione %d, offset=%d",
                            producerId,
                            record.key(),
                            record.value(),
                            TimestampFormatter.format(metadata.timestamp()), //NB: record.timestamp() è null, viene riempito dopo l'invio
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset()
                    );
                    System.out.println(log);
                } else {
                    System.err.println("[Producer " + producerId + "]: ❌ Errore nell'invio del messaggio:");
                    exception.printStackTrace();
                }
            });

            // TEST: Simula un errore inatteso prima del commit
            if (this.simulateErrors)  {
                if(random.nextInt(5) == 0) { // Questo blocco viene eseguito circa 1/5 delle volte
                    System.out.printf(ANSI_BLUE + "[Producer %s]" + ANSI_RESET + ": Simulo un errore durante l'invio del messaggio con key \"%s\" e value \"%s\", prima del commit\n", this.producerId, record.key(), record.value());
                    throw new RuntimeException("Errore simulato prima del commit!");
                }
            }

            // Commit della transazione se tutto è andato a buon fine
            producer.commitTransaction();
        } catch (ProducerFencedException e) {
            System.err.printf("[Producer %s]: ProducerFencedException: questo producer non è più valido. Chiudo.\n", producerId);
            producer.close();
        } catch (Exception e) {
            System.err.printf("[Producer %s]: ❌ Errore durante la transazione per il messaggio con key \"%s\" e value \"%s\", eseguo rollback%n", producerId, key, value);
            producer.abortTransaction();
        }
    }

    @Override
    public void close() {
        System.out.printf(ANSI_BLUE + "[Producer %s]" + ANSI_RESET + ": Chiudo il producer con Exactly-Once Semantic policy.\n",this. producerId);
        producer.close();
    }

}
