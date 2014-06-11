package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.MemoryRecords;

public class ProduceRequest {

    private final short acks;
    private final int timeout;
    private final Map<String, List<PartitionRecords>> records;

    public ProduceRequest(short acks, int timeout) {
        this.acks = acks;
        this.timeout = timeout;
        this.records = new HashMap<String, List<PartitionRecords>>();
    }

    public void add(TopicPartition tp, MemoryRecords recs) {
        List<PartitionRecords> found = this.records.get(tp.topic());
        if (found == null) {
            found = new ArrayList<PartitionRecords>();
            records.put(tp.topic(), found);
        }
        found.add(new PartitionRecords(tp, recs));
    }

    public Struct toStruct() {
        Struct produce = new Struct(ProtoUtils.currentRequestSchema(ApiKeys.PRODUCE.id));
        produce.set("acks", acks);
        produce.set("timeout", timeout);
        List<Struct> topicDatas = new ArrayList<Struct>(records.size());
        for (Map.Entry<String, List<PartitionRecords>> entry : records.entrySet()) {
            Struct topicData = produce.instance("topic_data");
            topicData.set("topic", entry.getKey());
            List<PartitionRecords> parts = entry.getValue();
            Object[] partitionData = new Object[parts.size()];
            for (int i = 0; i < parts.size(); i++) {
                ByteBuffer buffer = parts.get(i).records.buffer();
                buffer.flip();
                Struct part = topicData.instance("data")
                                       .set("partition", parts.get(i).topicPartition.partition())
                                       .set("record_set", buffer);
                partitionData[i] = part;
            }
            topicData.set("data", partitionData);
            topicDatas.add(topicData);
        }
        produce.set("topic_data", topicDatas.toArray());
        return produce;
    }

    private static final class PartitionRecords {
        public final TopicPartition topicPartition;
        public final MemoryRecords records;

        public PartitionRecords(TopicPartition topicPartition, MemoryRecords records) {
            this.topicPartition = topicPartition;
            this.records = records;
        }
    }

}
