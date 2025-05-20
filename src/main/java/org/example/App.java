package org.example;

import org.example.config.KafkaTopicInitializer;
import org.example.consumer.KafkaConsumerDemo;
import org.example.producer.KafkaProducerDemo;
import org.example.producer.KafkaProducerEOSDemo;

public class App {

    // Flag che abilita o disabilita il producer con policy EOS
    private static final Boolean eos = false;


    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        System.out.print("\n------\n");
        System.out.print("Kafka Producer and Consumer Demo");
        System.out.print("\n------\n");

        // Crea il topic se non esiste
        KafkaTopicInitializer.createTopicIfNotExists();

        // Lancia il consumer in un thread separato
        Thread consumerThread = new Thread(KafkaConsumerDemo::runConsumer);
        consumerThread.start();

        // Lancia il producer nel main thread
        if(eos){
            KafkaProducerEOSDemo.runTransactionalProducer();
        } else {
            KafkaProducerDemo.runProducer();
        }


        // Optional: ferma il consumer dopo un po'
        try {
            Thread.sleep(10000); // tempo per farlo girare
            System.out.print("Chiudo il Consumer...\n");
            consumerThread.interrupt(); // non ferma il consumer in modo "pulito", solo se lo modifichi
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }



}