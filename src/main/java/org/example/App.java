package org.example;

import org.example.config.KafkaTopicInitializer;
import org.example.consumer.KafkaConsumerDemo;
import org.example.msgGenerator.MessageGenerator;
import org.example.producer.KafkaProducerDemo;
import org.example.producer.KafkaProducerEOSDemo;
import org.example.producer.MessageProducer;

public class App {

    // Flag che abilita o disabilita il producer con policy EOS
    private static final Boolean eos = true;

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

        // Crea i producer
        MessageProducer standardProducer = new KafkaProducerDemo(TOPIC, BOOTSTRAP_SERVERS, "P-1");
        MessageProducer eosProducer = new KafkaProducerEOSDemo(TOPIC, BOOTSTRAP_SERVERS, "eos-producer-1", "P-eos-1", true);


        // Avvia due generatori messaggi che usano i due producer
        Thread generatorStandard = new Thread(new MessageGenerator(standardProducer, "Gen1"), "Generator-P1");
        generatorStandard.start();

        Thread generatorEOS = null;
        if(eos) {
            generatorEOS = new Thread(new MessageGenerator(eosProducer, "Gen2"), "Generator-EOS");
            generatorEOS.start();
        }


        // Attendi 10 secondi e poi ferma tutto
        try {
            Thread.sleep(10000);

            System.out.println("⏳ Attendo che il generator finisca e i producer siano chiusi (se non è già successo)...");
            generatorStandard.join();
            if(eos && generatorEOS!=null) {
                generatorEOS.join();
            }

            System.out.println("🛑 Chiudo il Consumer...");
            consumerRunnable.shutdown();
            consumerThread.join();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("✅ Programma terminato.");
    }



}