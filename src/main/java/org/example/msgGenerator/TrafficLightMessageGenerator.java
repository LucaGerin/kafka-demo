package org.example.msgGenerator;

import org.example.producer.MessageProducer;

public class TrafficLightMessageGenerator extends MessageGenerator {


    public TrafficLightMessageGenerator(MessageProducer producer, String generatorId) {
        super(producer, generatorId);
    }

    @Override
    protected String createKey() {
        return "traffic-light-" + this.getGeneratorId();
    }

    @Override
    protected String createValue(int i) {
        String[] states = {"VERDE", "GIALLO", "ROSSO"};
        String state = states[i % states.length];
        return String.format("traffic-light-%s: Stato semaforo %s", this.getGeneratorId(), state);
    }
}
