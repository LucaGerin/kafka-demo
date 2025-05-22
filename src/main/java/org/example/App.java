package org.example;

import com.example.avro.AircraftEvent;
import com.example.avro.AircraftKey;
import org.example.config.KafkaTopicInitializer;
import org.example.consumer.AircraftEventConsumer;
import org.example.consumer.StringMessageConsumerDemo;
import org.example.consumer.MessageConsumer;
import org.example.msgGenerator.AircraftMovementGenerator;
import org.example.msgGenerator.ThermometerMessageGenerator;
import org.example.msgGenerator.TrafficLightMessageGenerator;
import org.example.msgUser.AircraftEventPrinter;
import org.example.msgUser.StringMessagePrinter;
import org.example.producer.AircraftEventProducer;
import org.example.producer.StringProducerDemo;
import org.example.producer.StringProducerEOSDemo;
import org.example.producer.MessageProducer;

import java.util.ArrayList;
import java.util.List;

public class App {

    // Flag che abilita o disabilita il producer con policy EOS
    private static final Boolean eos = true;

    private static final String DEMO_TOPIC = "demo-topic";
    private static final String AIR_TOPIC = "aircraft-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "demo-group";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";


    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        System.out.print("\n------\n");
        System.out.print("Kafka Producer and Consumer Demo");
        System.out.print("\n------\n");

        // Crea il topic se non esiste
        KafkaTopicInitializer.createTopicIfNotExists();

        //  === Lista dei consumer ===
        List<MessageConsumer<?, ?>> consumers = new ArrayList<>();

        // Lancia il consumer di String su thread separato
        MessageConsumer<String, String> stringConsumer = new StringMessageConsumerDemo(DEMO_TOPIC, BOOTSTRAP_SERVERS, GROUP_ID, "C-1");
        consumers.add(stringConsumer);

        // Lancia il consumer di eventi Avro per gli aircraft su thread separato
        MessageConsumer<AircraftKey, AircraftEvent> airConsumer =
                new AircraftEventConsumer(AIR_TOPIC, BOOTSTRAP_SERVERS, GROUP_ID, SCHEMA_REGISTRY_URL, "C-Avro-1");
        consumers.add(airConsumer);

        // === Lista dei thread che usano i consumer ===
        List<Thread> consumerUserThreads = new ArrayList<>();

        // Lancia il printer associato al consumer di string
        Thread printerThread = new Thread(new StringMessagePrinter(stringConsumer, "Print-1"));
        printerThread.start();
        consumerUserThreads.add(printerThread);

        // Lancia il printer associato agli eventi Avro
        Thread aircraftPrinterThread = new Thread(new AircraftEventPrinter(airConsumer, "AirPrint-1"));
        aircraftPrinterThread.start();
        consumerUserThreads.add(aircraftPrinterThread);


        // === Lista dei producer ===
        List<MessageProducer<?, ?>> producers = new ArrayList<>();

        // Crea i producer
        MessageProducer<String, String> standardProducer = new StringProducerDemo(DEMO_TOPIC, BOOTSTRAP_SERVERS, "P-1");
        producers.add(standardProducer);
        MessageProducer<String, String> eosProducer = new StringProducerEOSDemo(DEMO_TOPIC, BOOTSTRAP_SERVERS, "eos-producer-1", "P-eos-1", true);
        producers.add(eosProducer);
        MessageProducer<AircraftKey, AircraftEvent> airProducer = new AircraftEventProducer(AIR_TOPIC, BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL,"A-1");
        producers.add(airProducer);

        // === Lista dei Thread dei generatori ===
        List<Thread> generatorThreads = new ArrayList<>();

        // Avvia i generatori messaggi che usano i due producer
        Thread generatorStandard = new Thread(new ThermometerMessageGenerator(standardProducer, "ThermoGen1"), "Thread-Generator-P1");
        generatorStandard.start();
        generatorThreads.add(generatorStandard);

        Thread generatorEOS = null;
        if(eos) {
            generatorEOS = new Thread(new TrafficLightMessageGenerator(eosProducer, "TFGen1"), "Thread-Generator-EOS1");
            generatorEOS.start();
            generatorThreads.add(generatorEOS);
        }

        Thread generatorAir = new Thread( new AircraftMovementGenerator(airProducer, "AirGen1"), "Thread-Generator-Air1");
        generatorAir.start();
        generatorThreads.add(generatorAir);


        // === Attendi 10 secondi e poi ferma tutto ===
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
            for (MessageProducer<?, ?> producer : producers) {
                try {
                    producer.close();
                } catch (Exception e) {
                    System.err.printf("Errore durante la chiusura del producer con id %s: %s\n", producer.getProducerId(), e.getMessage());
                }
            }

            System.out.println("⏳ Aspetto 2 secondi...");
            Thread.sleep(2000);

            System.out.println("⏳ Fermo gli user di messaggi...");
            for (Thread userThread : consumerUserThreads) {
                userThread.interrupt(); // segnala al thread che deve terminare
            }
            for (Thread userThread : consumerUserThreads) {
                try {
                    userThread.join(); // attende che il thread finisca
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("⛔ Interrotto mentre aspettavo uno user consumer.");
                }
            }

            System.out.println("🛑 Chiudo i consumer...");
            for (MessageConsumer<?, ?> consumer : consumers) {
                try {
                    consumer.close();
                } catch (Exception e) {
                    System.err.printf("Errore durante la chiusura del consumer con id %s: %s\n",
                            consumer.getConsumerId(), e.getMessage());
                }
            }



        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("✅ Programma terminato.");
    }



}