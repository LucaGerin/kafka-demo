package org.example.msgUser;

import com.example.avro.AircraftEvent;
import com.example.avro.AircraftKey;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.consumer.MessageConsumer;
import org.example.util.TimestampFormatter;

import java.util.List;

public class AircraftEventPrinter implements Runnable {

    private final MessageConsumer<AircraftKey, AircraftEvent> consumer;
    private final String printerId;

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_LIGHT_GREEN = "\u001B[38;5;120m"; // colore visibile ma distinto dal verde

    public AircraftEventPrinter(MessageConsumer<AircraftKey, AircraftEvent> consumer, String printerId) {
        this.consumer = consumer;
        this.printerId = printerId;
    }

    @Override
    public void run() {
        System.out.printf(ANSI_LIGHT_GREEN + "[Printer %s]" + ANSI_RESET + ": Avviato...\n", this.printerId);

        try {
            while (!Thread.currentThread().isInterrupted()) {
                List<ConsumerRecord<AircraftKey, AircraftEvent>> records = consumer.pollMessages();

                for (ConsumerRecord<AircraftKey, AircraftEvent> record : records) {

                    // Poiché il consumer ha "specific.avro.reader" = ture e sto usando maven avro plugin, posso recuperare key e value come oggetti java
                    // Altrimenti avrei dovuto fare accesso con GenricRecord.get("campo")
                    AircraftKey key = record.key();
                    AircraftEvent event = record.value();

                    // Uso un log per fare una stampa atomica di tutte le stringhe che metto dentro di esso
                    StringBuilder log = new StringBuilder();
                    log.append(String.format(ANSI_LIGHT_GREEN + "[AircraftPrinter %s]" + ANSI_RESET + ": Evento %s per aereo %s (%s - %s)\n",
                            printerId,
                            event.getEventType(),
                            key.getTailNumber(),
                            key.getModel(),
                            key.getAirline()));

                    log.append(String.format("\t   • Aeroporto: %s\n", event.getAirportCode()));
                    log.append(String.format("\t   • Orario schedulato: %s\n", TimestampFormatter.format(event.getScheduledTime())));
                    log.append(String.format("\t   • Orario effettivo : %s\n", TimestampFormatter.format(event.getActualTime())));
                    log.append(String.format("\t   • Topic=%s | Partizione=%d | Offset=%d | Timestamp=%s\n",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            TimestampFormatter.format(record.timestamp())));

                    System.out.print(log);

                }

                Thread.sleep(500); // per non ciclare a vuoto
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.printf(ANSI_LIGHT_GREEN + "[AircraftPrinter %s]" + ANSI_RESET + ": Interrotto.\n", printerId);
        } finally {
            System.out.printf(ANSI_LIGHT_GREEN + "[AircraftPrinter %s for consumer %s]" + ANSI_RESET + ": Terminato.\n", this.printerId, consumer.getConsumerId());
        }
    }
}
