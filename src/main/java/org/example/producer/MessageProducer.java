package org.example.producer;

public interface MessageProducer<K, V> extends AutoCloseable {

    void sendMessage(K key, V value);

    String getProducerId();

    @Override
    void close(); // obbliga le implementazioni a gestire le risorse

}