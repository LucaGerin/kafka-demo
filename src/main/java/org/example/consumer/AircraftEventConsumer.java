package org.example.consumer;

import com.example.avro.AircraftEvent;
import com.example.avro.AircraftKey;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.*;

public class AircraftEventConsumer implements MessageConsumer<AircraftKey, AircraftEvent>{

    private final String consumerId;
    private final KafkaConsumer<AircraftKey, AircraftEvent> consumer;
    private final String topic;

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";

    public AircraftEventConsumer(String topic, String bootstrapServers, String groupId, String schemaRegistryUrl, String consumerId) {
        this.topic = topic;
        this.consumerId = consumerId;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        // "schema.registry.url" è l'url dove è esposto il registry
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        /* "specific.avro.reader" forza l'uso delle classi generate da Avro invece del tipo generico GenericRecord
         * Quando un consumer Kafka usa Avro, può deserializzare i dati in due modi:
         * 1. GenericRecord (DEFAULT):
         *    - Kafka usa una rappresentazione generica dello schema, quindi il messaggio viene restituito come oggetto di tipo GenericRecord.
         *    - È necessario in questo caso accedere ai campi con record.get("campo"). Questo approccio è Utile se non si hanno le classi Java generate dai file .avsc.
         * 2. SpecificRecord (con questa config = true):
         *    - Kafka restituisce oggetti Java specifici (es. AircraftEvent, AircraftKey) generati dallo schema Avro con il plugin Maven o il tool Avro.
         *    - Questo approccio è più sicuro e leggibile: si accede ai campi con i metodi getter (es. getEventId()). Evita errori di digitazione e migliora l’autocompletamento.
         *
         * Impostando questa proprietà su `true`, abiliti il deserializer a restituire direttamente istanze delle classi Avro generate.
         * Se non abiliti questa configurazione, il consumer riceverà oggetti GenericRecord anche se hai generato le classi Java.
         */
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        props.put(KafkaAvroDeserializerConfig.KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName()); // "key.subject.name.strategy"
        props.put(KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName()); // "value.subject.name.strategy"

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic), createRebalanceListener());

        System.out.println(ANSI_GREEN + "[Consumer " + consumerId + "]" + ANSI_RESET + ": Consumer avviato. In attesa di messaggi...");
    }

    private ConsumerRebalanceListener createRebalanceListener() {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.printf(ANSI_GREEN + "[Consumer %s]" + ANSI_RESET + ":⚠️  Partizioni revocate: %s\n", consumerId, partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.printf(ANSI_GREEN + "[Consumer %s]" + ANSI_RESET + ": Partizioni assegnate: %s\n", consumerId, partitions);
                for (TopicPartition partition : partitions) {
                    try {
                        long offset = consumer.position(partition);
                        System.out.println(ANSI_GREEN + "[Consumer " + consumerId + "]" + ANSI_RESET + ": Partition " + partition.partition() + " - Starting offset: " + offset);
                    } catch (Exception e) {
                        System.err.printf("[Consumer " + consumerId + "]: Errore nel recupero offset partizione %d: %s%n", partition.partition(), e.getMessage());
                    }
                }
            }
        };
    }

    @Override
    public List<ConsumerRecord<AircraftKey, AircraftEvent>> pollMessages() {

        List<ConsumerRecord<AircraftKey, AircraftEvent>> results = new ArrayList<>();

        try {
            ConsumerRecords<AircraftKey, AircraftEvent> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<AircraftKey, AircraftEvent> record : records) {
                results.add(record);
            }
        } catch (WakeupException | InterruptException e) {
            Thread.currentThread().interrupt(); // chiusura controllata
        }
        return results;
    }

    @Override
    public String getConsumerId() {
        return consumerId;
    }

    @Override
    public void close() {
        consumer.close();
        System.out.println(ANSI_GREEN + "[Consumer " + this.consumerId + "]" + ANSI_RESET + ": Consumer chiuso.");
    }

}
