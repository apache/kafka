package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class SimulationUtils {
    public static <K, V> Map.Entry<K, V> randomEntry(Random random, Map<K, V> map) {
        if (map.isEmpty()) {
            throw new IllegalArgumentException("Cannot get random entry from empty map");
        }

        int randomIdx = random.nextInt(map.size());
        int idx = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (randomIdx == idx) {
                return entry;
            } else {
                idx++;
            }
        }
        throw new IllegalArgumentException("Cannot reach here");
    }

    public static String batchValueRangeAsString(RecordBatch batch) {
        StringBuilder str = new StringBuilder();
        Iterator<Record> iterator = batch.iterator();
        while (iterator.hasNext()) {
            Record record = iterator.next();
            if (str.length() == 0) {
                long value = record.value().duplicate().getLong();
                str.append("[").append(value).append(", ");
            }

            if (!iterator.hasNext()) {
                long value = record.value().duplicate().getLong();
                str.append(value).append("]");
            }
        }

        return str.toString();
    }
}
