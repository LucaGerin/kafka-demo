package org.example;

import org.example.config.KafkaTopicInitializer;
import org.example.consumer.KafkaConsumerDemo;
import org.example.msgGenerator.MessageGenerator;
import org.example.msgGenerator.ThermometerMessageGenerator;
import org.example.msgGenerator.TrafficLightMessageGenerator;
import org.example.producer.KafkaProducerDemo;
import org.example.producer.KafkaProducerEOSDemo;
import org.example.producer.MessageProducer;

import java.util.ArrayList;
import java.util.List;

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

        // Lista dei producer
        List<MessageProducer> producers = new ArrayList<>();

        // Crea i producer
        MessageProducer standardProducer = new KafkaProducerDemo(TOPIC, BOOTSTRAP_SERVERS, "P-1");
        producers.add(standardProducer);
        MessageProducer eosProducer = new KafkaProducerEOSDemo(TOPIC, BOOTSTRAP_SERVERS, "eos-producer-1", "P-eos-1", true);
        producers.add(eosProducer);

        // Lista dei Thread dei generatori
        List<Thread> generatorThreads = new ArrayList<>();

        // Avvia due generatori messaggi che usano i due producer
        Thread generatorStandard = new Thread(new ThermometerMessageGenerator(standardProducer, "Thermo1"), "Thread-Generator-P1");
        generatorStandard.start();
        generatorThreads.add(generatorStandard);

        Thread generatorEOS = null;
        if(eos) {
            generatorEOS = new Thread(new TrafficLightMessageGenerator(eosProducer, "TF1"), "Thread-Generator-EOS");
            generatorEOS.start();
            generatorThreads.add(generatorEOS);
        }


        // Attendi 10 secondi e poi ferma tutto
        try {
            Thread.sleep(10000);

            System.out.println("⏳ Attendo che i generator finiscano (se non hanno già terminato)...");

            for (Thread generatorThread : generatorThreads) {
                try {
                    generatorThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("⛔ Interrotto mentre aspettavo un generator.");
                }
            }

            System.out.println("🔒 Chiudo i producer condivisi...");
            for (MessageProducer producer : producers) {
                try {
                    producer.close();
                } catch (Exception e) {
                    System.err.printf("Errore durante la chiusura del producer con id %s: %s\n", producer.getProducerId(), e.getMessage());
                }
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