package org.example.msgGenerator;

import org.example.producer.MessageProducer;

public abstract class MessageGenerator<K, V> implements Runnable {

    private final MessageProducer<K, V> producer;
    private String generatorId;

    public MessageGenerator(MessageProducer<K, V> producer, String generatorId) {
        this.producer = producer;
        this.generatorId = generatorId;
    }

    public String getGeneratorId() {
        return generatorId;
    }

    // Metodo astratto da implementare per generare messaggi
    protected abstract K createKey();
    protected abstract V createValue(int i);


    @Override
    public void run() {
        try {
            for (int i = 1; i <= 5; i++) {
                K key = createKey();
                V value = createValue(i);

                // Usa il producer per mandare un messaggio
                producer.sendMessage(key, value);

                // Simula tempo tra i messaggi
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            System.out.printf("[Message Generator %s]: Ho concluso la generazione di messaggi e per il producer %s.\n", this.generatorId, this.producer.getProducerId());
        }
    }
}
