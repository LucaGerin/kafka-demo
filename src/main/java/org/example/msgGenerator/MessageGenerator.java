package org.example.msgGenerator;

import org.example.producer.MessageProducer;

public abstract class MessageGenerator implements Runnable {

    private final MessageProducer producer;
    private String generatorId;

    public MessageGenerator(MessageProducer producer, String generatorId) {
        this.producer = producer;
        this.generatorId = generatorId;
    }

    public String getGeneratorId() {
        return generatorId;
    }

    // Metodo astratto da implementare per generare messaggi
    protected abstract String createKey();
    protected abstract String createValue(int i);


    @Override
    public void run() {
        try {
            for (int i = 1; i <= 5; i++) {
                String key = createKey();
                String value = createValue(i);

                // Usa il prpducer per mandare un messaggio
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
