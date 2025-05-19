package org.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {

    private final static String TOPIC = "demo-topic";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void runProducer() {

        // ------- Configura le proprietà del Kafka Producer -------
        //Crea un oggetto Properties, che è una mappa chiave/valore usata da Kafka per configurare il producer.
        Properties props = new Properties();
        // Specifica l'indirizzo del broker Kafka a cui il producer deve connettersi, "bootstrap" perché serve solo per iniziare in quanto Kafka poi scopre gli altri broker automaticamente
        // BOOTSTRAP_SERVERS_CONFIG è una costante che equivale a "bootstrap.servers"
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // Dice al producer come serializzare la key del messaggio
        // In questo caso si è scelto StringSerializer, che converte le chiavi da String a byte (byte[]) prima di inviarle.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Dice al producer come serializzare il value del messaggio
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(props)) { //try-with-resources esegue producer.close() automaticamente alla fine
            for (int i = 1; i <= 10; i++) {

                // Crea il value
                String value = "Messaggio numero " + i;

                // Crea il record da spedire passando topic, key, value
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, Integer.toString(i), value);

                // Invia in modo asincrono il record e gestisce i metadata che sono ritornati o le exception
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("✅ Messaggio con key=\"%s\" inviato al topic \"%s\", partizione %d, offset=%d%n",
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
    }
}
