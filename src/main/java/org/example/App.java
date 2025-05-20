package org.example;

import org.example.config.KafkaTopicInitializer;
import org.example.consumer.KafkaConsumerDemo;
import org.example.producer.KafkaProducerDemo;
import org.example.producer.KafkaProducerEOSDemo;

public class App {

    // Flag che abilita o disabilita il producer con policy EOS
    private static final Boolean eos = false;

    private static final String TOPIC = "demo-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "demo-group";


    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        System.out.print("\n------\n");
        System.out.print("Kafka Producer and Consumer Demo");
        System.out.print("\n------\n");

        // Crea il topic se non esiste
        KafkaTopicInitializer.createTopicIfNotExists();

        // Lancia il consumer su thread separato
        KafkaConsumerDemo consumerRunnable = new KafkaConsumerDemo(TOPIC, BOOTSTRAP_SERVERS, GROUP_ID, "C-1");
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        // Lancia il producer nel thread principale
        if (eos) {
            KafkaProducerEOSDemo producer = new KafkaProducerEOSDemo(TOPIC, BOOTSTRAP_SERVERS, "eos-producer-1", "P-eos-1", true);
            producer.runTransactionalProducer();
        } else {
            KafkaProducerDemo producer = new KafkaProducerDemo(TOPIC, BOOTSTRAP_SERVERS, "P-1");
            producer.runProducer();
        }


        // Aspetta 10 secondi e ferma il consumer
        try {
            Thread.sleep(10000); // tempo per farlo girare
            System.out.print("Chiudo il Consumer...\n");
            consumerRunnable.shutdown();     // shutdown "gentile"
            consumerThread.join();           // aspetta che il consumer chiuda
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }



}