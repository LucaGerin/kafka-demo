package org.example.msgGenerator;

import org.example.producer.MessageProducer;

public class MessageGenerator implements Runnable {

    private final MessageProducer producer;
    private String generatorId;

    public MessageGenerator(MessageProducer producer, String generatorId) {
        this.producer = producer;
        this.generatorId = generatorId;
    }

    public String getGeneratorId() {
        return generatorId;
    }


    @Override
    public void run() {
        try {
            for (int i = 1; i <= 5; i++) {
                String key = "device-" + i;
                String value = "Messaggio n " + i + ", inviato usando producer " + producer.getProducerId();

                // Usa il prpducer per mandare un messaggio
                producer.sendMessage(key, value);

                // Simula tempo tra i messaggi
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            try {
                System.out.printf("[Message Generator %s]: Chiedo la chiusura del producer con id %s\n", this.generatorId, this.producer.getProducerId());
                producer.close();
            } catch (Exception e) {
                System.err.printf("[Message Generator %s]: Errore durante la chiusura del producer con id %s : %s\n", this.generatorId, this.producer.getProducerId(), e.getMessage());
            }
        }
    }
}
