package org.example.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomPartitioner implements Partitioner {

    // Round-robin index usato quando la chiave è null
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void configure(Map<String, ?> configs) {
        // Puoi leggere config personalizzate qui se vuoi
    }

    // Metodo che definisce in che partizione deve finire un messaggio
    // In questa implementazione copiamo la strategia di default, tranne se il messaggio hail prefisso "[VERY IMPORTANT]", in tal caso la partizione è la 0
    @Override
    public int partition(String topic,
                         Object key,
                         byte[] keyBytes,
                         Object value,
                         byte[] valueBytes,
                         Cluster cluster) {

        // Recupera il numero di partizioni di un topic
        int numPartitions = cluster.partitionCountForTopic(topic);

        // Partizione fissa per messaggi VIP
        if (key instanceof String && ((String) key).startsWith("[VERY IMPORTANT]")) {
            return 0;
        }

        // Distribuzione default di kafka replicata: hash della chiave o round robin se chiave null
        int partition;
        if (keyBytes == null) {
            // Round-robin per chiave nulla (comportamento del DefaultPartitioner)
            return counter.getAndIncrement() % numPartitions;
        } else {
            // Hash deterministico per chiave non nulla
            int hash = org.apache.kafka.common.utils.Utils.murmur2(keyBytes);
            partition = Math.abs(hash) % numPartitions;
        }
        return partition;
    }

    @Override
    public void close() {
        // Rilascia eventuali risorse se necessario
    }

    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        // Metodo opzionale introdotto in Kafka 2.4 per ottimizzazioni
    }
}
