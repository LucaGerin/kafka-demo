package org.example.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
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
    // NB: Con un partitioner personalizzato non è necessario basarsi necessariamente sulla key, posso usare qualunque cosa tra quelle che mi sono passate qui come parametri!
    @Override
    public int partition(String topic,
                         Object key,
                         byte[] keyBytes,
                         Object value,
                         byte[] valueBytes,
                         Cluster cluster) {

        // Recupera il numero di partizioni di un topic
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if (keyBytes != null && !(key instanceof String)){
            throw new InvalidRecordException("Record did not have a string key");
        }

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

            // Se volessi usare tutte le partizioni tranne la 0:
            // partition = ( Math.abs(hash) % (numPartitions-1) ) + 1;
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
