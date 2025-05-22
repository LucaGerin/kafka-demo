package org.example.msgGenerator;

import org.example.producer.MessageProducer;

public class ThermometerMessageGenerator extends MessageGenerator<String, String> {


    public ThermometerMessageGenerator(MessageProducer<String, String> producer, String generatorId) {
        super(producer, generatorId);
    }

    @Override
    protected String createKey() {
        return "thermometer-" + this.getGeneratorId();
    }

    @Override
    protected String createValue(int i) {
        int temperature = 18 + (int)(Math.random() * 10); // Temperatura casuale tra 18 e 27
        return String.format("thermometer-%s: Temperatura rilevata: %d°C", this.getGeneratorId(), temperature);
    }
}
