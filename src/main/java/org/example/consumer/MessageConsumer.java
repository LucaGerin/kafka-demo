package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Map;

public interface MessageConsumer<K, V> extends AutoCloseable {

    String getConsumerId();

    /**
     * Legge i messaggi dal topic e li restituisce come lista.
     * Questo metodo può bloccare brevemente (es. poll).
     */
    List<ConsumerRecord<K, V>> pollMessages();

    @Override
    void close(); // per eventuali risorse da liberare (es. KafkaConsumer.close())

}
