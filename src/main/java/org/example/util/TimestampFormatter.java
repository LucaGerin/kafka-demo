package org.example.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TimestampFormatter {

    // Metodo pubblico e statico, utilizzabile da qualsiasi classe
    public static String format(long kafkaTimestamp) {
        // Converte il timestamp in un formato più leggibile
        Instant instant = Instant.ofEpochMilli(kafkaTimestamp);
        return DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
                .withZone(ZoneId.systemDefault())
                .format(instant);
    }
}
