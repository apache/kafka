package org.apache.kafka.clients.consumer;

import static org.junit.Assert.*;

import java.util.Iterator;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

public class MockConsumerTest {
    
    private MockConsumer<String, String> consumer = new MockConsumer<String, String>();

    @Test
    public void testSimpleMock() {
        consumer.subscribe("topic");
        assertEquals(0, consumer.poll(1000).count());
        ConsumerRecord<String, String> rec1 = new ConsumerRecord<String, String>("test", 0, 0, "key1", "value1");
        ConsumerRecord<String, String> rec2 = new ConsumerRecord<String, String>("test", 0, 1, "key2", "value2");
        consumer.addRecord(rec1);
        consumer.addRecord(rec2);
        ConsumerRecords<String, String> recs = consumer.poll(1);
        Iterator<ConsumerRecord<String, String>> iter = recs.iterator();
        assertEquals(rec1, iter.next());
        assertEquals(rec2, iter.next());
        assertFalse(iter.hasNext());
        assertEquals(1L, consumer.position(new TopicPartition("test", 0)));
        consumer.commit(CommitType.SYNC);
        assertEquals(1L, consumer.committed(new TopicPartition("test", 0)));
    }

}
