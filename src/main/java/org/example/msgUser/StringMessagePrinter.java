package org.example.msgUser;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.consumer.MessageConsumer;
import org.example.util.TimestampFormatter;

import java.util.List;
import java.util.Map;

public class StringMessagePrinter implements Runnable{

    private final MessageConsumer<String, String> consumer;
    private final String printerId;

    // Codici ANSI per il colore
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_LIGHT_GREEN = "\u001B[38;5;120m";


    public StringMessagePrinter(MessageConsumer<String, String> consumer, String printerId) {
        this.consumer = consumer;
        this.printerId = printerId;
    }

    @Override
    public void run() {
        System.out.printf(ANSI_LIGHT_GREEN + "[Printer for consumer %s]" + ANSI_RESET + ": Avviato...\n", consumer.getConsumerId());

        try {
            while (!Thread.currentThread().isInterrupted()) {
                List<ConsumerRecord<String, String>> messages = consumer.pollMessages();

                for (ConsumerRecord<String, String> record : messages) {

                    // Stampa il messaggio
                    System.out.printf(ANSI_LIGHT_GREEN +"[Printer " + printerId + "]" + ANSI_RESET + ": ⬇️ Ricevuto: topic=%s, partition=%d, offset=%d, \n\t\t\t\tkey=%s, timestamp=%s,\n\t\t\t\tvalue=\"%s\" \n",
                            record.topic(), record.partition(), record.offset(), record.key(), TimestampFormatter.format(record.timestamp()), record.value());
                }

                Thread.sleep(500); // per non ciclare a vuoto
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.printf(ANSI_LIGHT_GREEN +"[Printer " + printerId + "]" + ANSI_RESET + ": Interrotto.\n");
        } finally {
            System.out.printf(ANSI_LIGHT_GREEN + "[Printer for consumer %s]" + ANSI_RESET + ": Terminato.\n", consumer.getConsumerId());
        }
    }

}
