package org.example.producer;

public interface MessageProducer extends AutoCloseable {

    void sendMessage(String key, String value);

    String getProducerId();

    @Override
    void close(); // obbliga le implementazioni a gestire le risorse

}