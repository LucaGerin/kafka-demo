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

        // ------- Configura le proprietà del Kafka Consumer -------
        //Crea un oggetto Properties, che è una mappa chiave/valore usata da Kafka per configurare il consumer.
        Properties props = new Properties();
        // Specifica l'indirizzo del broker Kafka a cui il producer deve connettersi, "bootstrap" perché serve solo per iniziare in quanto Kafka poi scopre gli altri broker automaticamente
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // Dice al consumer come convertire la key dei messaggi da byte[] a oggetti Java
        // Qui si usa StringDeserializer, quindi Kafka converte le chiavi dei messaggi in String
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Dice al consumer come convertire il value dei messaggi da byte[] a oggetti Java
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Imposta l'ID del gruppo consumer
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        // Specifica cosa fare se il gruppo consumer non ha offset salvato in precedenza:
        // - "earliest": consuma dall'inizio del topic
        // - "latest": consuma solo i nuovi messaggi
        // - "none": solleva un errore
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        // Crea un KafkaConsumer che verrà chiuso automaticamente alla fine del blocco (try-with-resources)
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // Si iscrive a una lista di topic (in questo caso uno solo)
            consumer.subscribe(Collections.singletonList(TOPIC));
            System.out.println("✅ Consumer avviato. In attesa di messaggi...");

            // Ciclo principale: continua a leggere finché il thread non viene interrotto
            while (!Thread.currentThread().isInterrupted()) {
                // Effettua il polling: attende fino a 1 secondo per ricevere nuovi messaggi
                // Il consumer "interroga" Kafka per nuovi messaggi:
                // - Se ce ne sono, li riceve
                // - Se non ce ne sono, riceve una risposta vuota
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                // Elabora ogni messaggio ricevuto
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("⬇️  Ricevuto: key=%s, value=%s, offset=%d, partition=%d%n",
                            record.key(), record.value(), record.offset(), record.partition());
                }
            }

        } catch (WakeupException e) {
            // Eccezione normale per "svegliare" il consumer quando deve essere chiuso in modo asincrono
            System.out.println("⚠️ Consumer svegliato per chiusura.");

        } catch (InterruptException e) {
            // Il thread è stato interrotto (es. da consumerThread.interrupt())
            System.out.println("----------------");
            System.out.println("ℹ️  Consumer interrotto.");

        } catch (Exception e) {
            // Qualsiasi altro errore imprevisto durante l'esecuzione
            System.err.println("❌ Errore nel consumer:");
            e.printStackTrace();

        } finally {
            // Chiusura finale
            System.out.println("🔚 Consumer chiuso.");
        }

    }
}
