package org.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {

    private final static String TOPIC = "demo-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void runProducer() {

        /*
         * === Kafka Producer Configuration ===
         *
         * bootstrap.servers
         * - Descrizione: Elenco di host:porta dei broker Kafka usati per stabilire la connessione iniziale al cluster.
         *
         * key.serializer
         * Classe utilizzata per serializzare la chiave del messaggio.
         * Deve implementare l'interfaccia org.apache.kafka.common.serialization.Serializer.
         *
         * value.serializer
         * Classe utilizzata per serializzare il valore del messaggio.
         * Deve anch'essa implementare l'interfaccia Serializer.
         *
         * compression.type
         * Specifica come i dati devono essere compressi prima dell'invio.
         * Valori supportati: none, snappy, gzip, lz4, zstd.
         * Il valore di default è none.
         * La compressione viene applicata a batch di record.
         *
         * acks
         * Definisce quanti acknowledgment il producer richiede dal leader prima di considerare una richiesta completata.
         * Impatta sulla durabilità dei record.
         * Valori possibili:
         *     - acks=0 -> Il producer NON aspetta alcun acknowledgment dal broker.
         *              Massime prestazioni, ma nessuna garanzia di consegna.
         *     - acks=1 -> Il producer aspetta che SOLO il leader abbia scritto il messaggio
         *              nel log locale. Buon compromesso tra velocità e sicurezza. E' l'opzione di DEFAULT.
         *     - acks=all -> Il producer aspetta che TUTTE le repliche sincronizzate (in-sync replicas)
         *              abbiano confermato di aver ricevuto il messaggio.
         *              Massima garanzia di durabilità, ma più lento.
         */


        // ------- Configura le proprietà del Kafka Producer che sarà creato con new KafkaProducer<>(props) -------
        // NB: Si usa al posto del nome delle properties la classe ProducerConfig. Per esempio, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG equivale a "bootstrap.servers" ma è controllato a compile time.

        // Crea un oggetto Properties, che è una mappa chiave/valore usata da Kafka per configurare il producer.
        Properties props = new Properties();
        //  Specifica l'indirizzo del broker (o, sarebbe meglio, lista di broker) Kafka a cui il producer deve connettersi, "bootstrap" perché serve solo per iniziare in quanto Kafka poi scopre gli altri broker automaticamente
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // Dice al producer come serializzare la key del messaggio
        // In questo caso si è scelto StringSerializer, che converte le chiavi da String a byte (byte[]) secondo UTF8.
        // Altri serializers disponibili: ByteArraySerializer, IntegerSerializer, LongSerializer
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Dice al producer come serializzare il value del messaggio
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Configurazioni di ottimizzazione:
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);        // linger.ms: Tempo di attesa prima di inviare un batch, per raggruppare più record (default: 0)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);    // batch.size: Dimensione massima di un batch in byte (default: 16384 = 16 KB)
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // buffer.memory: Memoria totale disponibile per buffering dei record (default: 33554432 = 32 MB)


        try (Producer<String, String> producer = new KafkaProducer<>(props)) { //try-with-resources esegue producer.close() automaticamente alla fine
            for (int i = 1; i <= 10; i++) {

                // Crea il record da spedire passando topic, key, value
                String key = Integer.toString(i);
                String value = "Messaggio numero " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

                /*
                 * Invio di records da parte di un Kafka producer, può essere::
                 *
                 * 1. Invio ASINCRONO (default):
                 *    - Il metodo Producer<K, V>.send() restituisce immediatamente un oggetto Future<RecordMetadata>.
                 *          Future<RecordMetadata> send(ProducerRecord<K, V> var1);
                 *          Future<RecordMetadata> send(ProducerRecord<K, V> var1, Callback var2);
                 *    - Il messaggio viene inviato "in background", e l'applicazione continua a eseguire senza aspettare la conferma.
                 *      In particolare, il messaggio viene messo in un buffer locale e un altro thread si occupa dell'invio di questi messaggi (anche in batch). L'invio si può forzare con il metodo:
                 *          Producer<K, V>.flush();
                 *    - Preferisci l'invio asincrono per alte prestazioni e throughput.
                 *    - È possibile registrare una callback per gestire successi o errori:
                 *
                 *    producer.send(record, (metadata, exception) -> {
                 *        if (exception == null) {
                 *            System.out.println("Messaggio inviato con successo: " + metadata);
                 *        } else {
                 *            exception.printStackTrace();
                 *        }
                 *    });
                 *
                 * 2. Invio SINCRONO:
                 *    - Si forza l'attesa del completamento dell'invio tramite `.get()` sul Future.
                 *    - L'esecuzione si blocca finché il broker non risponde (o genera un errore).
                 *    - È utile quando è necessario garantire l'invio prima di procedere.
                 *    - Usa l'invio sincrono quando hai bisogno di garanzie forti sull'invio (es. logging critico o transazioni).
                 *
                 *    try {
                 *        RecordMetadata metadata = producer.send(record).get(); // invio sincrono
                 *        System.out.println("Messaggio inviato: " + metadata);
                 *    } catch (Exception e) {
                 *        e.printStackTrace();
                 *    }
                 *
                 */


                // Invia in modo asincrono il record e gestisce i metadata che sono ritornati o le exception
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("[Producer]: ✅ Messaggio con key=\"%s\" inviato al topic \"%s\", partizione %d, offset=%d%n",
                                record.key(), metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });


                // Simula tempo tra i messaggi
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.print("[Producer]: Ho finito di produrre messaggi.\n");
    }
}
