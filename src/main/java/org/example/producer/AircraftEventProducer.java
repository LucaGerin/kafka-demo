package org.example.producer;

import com.example.avro.AircraftKey;
import com.example.avro.AircraftEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.example.util.TimestampFormatter;

import java.util.Properties;

public class AircraftEventProducer implements MessageProducer<AircraftKey, AircraftEvent> {

    private final KafkaProducer<AircraftKey, AircraftEvent> producer;
    private final String topic;
    private final String producerId;

    // Codici ANSI per il colore
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_LIGHT_CYAN = "\u001B[38;5;153m";

    public AircraftEventProducer(String topic, String bootstrapServers, String schemaRegistryUrl, String producerId) {
        this.topic = topic;
        this.producerId = producerId;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Serializza la key e il value del messaggio utilizzando Avro e lo Schema Registry
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Specifica l'URL dello Schema Registry per la registrazione e il recupero degli schemi Avro
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        /*
         * Imposta la strategia di naming per i subject dello Schema Registry relativi alla CHIAVE del messaggio Kafka.
         * 1. TopicNameStrategy (DEFAULT):
         *    - Registra gli schemi con il nome <topic>-key o <topic>-value.
         *    - Esempio: per il topic "aircraft-events", il subject sarà "aircraft-events-key".
         *    - Ogni topic ha un proprio subject anche se lo schema è identico.
         *    - Vantaggi: utile quando gli schemi cambiano tra topic diversi.
         *    - Svantaggi: genera duplicati se lo stesso schema viene riutilizzato su più topic.
         * 2. RecordNameStrategy:
         *    - Registra lo schema usando il nome completo (fully qualified) del record Avro: <namespace>.<name>.
         *    - Esempio: com.example.avro.AircraftKey
         *    - Vantaggi: consente il riutilizzo dello stesso schema su più topic.
         *    - Requisiti: ogni record Avro deve avere un namespace e un nome univoco.
         * 3. TopicRecordNameStrategy:
         *    - Registra lo schema combinando il nome del topic e il nome completo del record: <topic>-<namespace>.<name>.
         *    - Esempio: aircraft-events-com.example.avro.AircraftKey
         *    - Vantaggi: consente una tracciabilità per topic pur mantenendo il riferimento al record.
         *    - Svantaggi: riduce il riutilizzo del subject rispetto a RecordNameStrategy.
         */
        // In questo caso si usa RecordNameStrategy per registrare la key come com.example.avro.AircraftKey,
        // rendendo possibile il riutilizzo dello schema tra topic diversi.
        props.put("key.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");
        // Facciamo lo stesso anche per il value
        props.put("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");


        this.producer = new KafkaProducer<>(props);
    }


    @Override
    public void sendMessage(AircraftKey key, AircraftEvent value) {
        ProducerRecord<AircraftKey, AircraftEvent> record = new ProducerRecord<>(topic, key, value);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                String log = String.format(
                        ANSI_LIGHT_CYAN + "[Producer %s]" + ANSI_RESET + ": ✅ Messaggio con key=\"%s\", \n\t\t\t\tvalue=\"%s\", \n\t\t\t\ttimestamp=%s \n\t\t\t\tinviato al topic \"%s\", partizione %d, offset=%d",
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
                System.err.println("[Producer " + producerId + "] Errore nell'invio: " + exception.getMessage());
            }
        });
    }

    @Override
    public String getProducerId() {
        return producerId;
    }

    @Override
    public void close() {
        System.out.println("[Producer " + producerId + "] Chiusura del producer...");
        producer.flush();
        producer.close();
    }
}
