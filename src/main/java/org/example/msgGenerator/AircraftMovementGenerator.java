package org.example.msgGenerator;

import com.example.avro.*;
import org.example.producer.MessageProducer;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class AircraftMovementGenerator extends MessageGenerator<AircraftKey, AircraftEvent> {

    private final Random random = new Random();

    private static final String[] MODELS = {"A320", "B737", "B777", "A350", "E190"};
    private static final String[] AIRPORTS = {"FCO", "JFK", "CDG", "LHR", "DXB", "AMS"};
    private static final Airline[] AIRLINES = Airline.values();
    private static final String[] TAIL_NUMBERS = {"I-ABCD", "I-EFGH", "I-IJKL", "I-MNOP", "I-QRST", "I-UVWX"};

    public AircraftMovementGenerator(MessageProducer<AircraftKey, AircraftEvent> producer, String generatorId) {
        super(producer, generatorId);
    }

    @Override
    protected AircraftKey createKey() {
        String tailNumber = TAIL_NUMBERS[random.nextInt(TAIL_NUMBERS.length)];
        String model = MODELS[random.nextInt(MODELS.length)];
        Airline airline = AIRLINES[random.nextInt(AIRLINES.length)];

        return AircraftKey.newBuilder()
                .setTailNumber(tailNumber)
                .setModel(model)
                .setAirline(airline)
                .build();
    }

    @Override
    protected AircraftEvent createValue(int i) {
        String eventId = UUID.randomUUID().toString();
        AircraftEventType eventType = random.nextBoolean() ? AircraftEventType.DEPARTURE : AircraftEventType.ARRIVAL;
        String airportCode = AIRPORTS[random.nextInt(AIRPORTS.length)];
        long now = Instant.now().toEpochMilli();
        long delay = random.nextLong(30 * 60 * 1000L); // fino a 30 min in ms

        long scheduled = now;
        long actual = eventType == AircraftEventType.DEPARTURE
                ? scheduled + delay
                : scheduled - delay;

        return AircraftEvent.newBuilder()
                .setEventId(eventId)
                .setEventType(eventType)
                .setAirportCode(airportCode)
                .setScheduledTime(scheduled)
                .setActualTime(actual)
                .build();
    }
}
