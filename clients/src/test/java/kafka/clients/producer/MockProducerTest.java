package kafka.clients.producer;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import kafka.common.Cluster;
import kafka.common.Node;
import kafka.common.PartitionInfo;
import kafka.common.Serializer;
import kafka.common.StringSerialization;

import org.junit.Test;

public class MockProducerTest {

    @Test
    public void testAutoCompleteMock() {
        MockProducer producer = new MockProducer(true);
        ProducerRecord record = new ProducerRecord("topic", "key", "value");
        RecordSend send = producer.send(record);
        assertTrue("Send should be immediately complete", send.completed());
        assertFalse("Send should be successful", send.hasError());
        assertEquals("Offset should be 0", 0, send.offset());
        assertEquals("We should have the record in our history", asList(record), producer.history());
        producer.clear();
        assertEquals("Clear should erase our history", 0, producer.history().size());
    }

    public void testManualCompletion() {
        MockProducer producer = new MockProducer(false);
        ProducerRecord record1 = new ProducerRecord("topic", "key1", "value1");
        ProducerRecord record2 = new ProducerRecord("topic", "key2", "value2");
        RecordSend send1 = producer.send(record1);
        assertFalse("Send shouldn't have completed", send1.completed());
        RecordSend send2 = producer.send(record2);
        assertFalse("Send shouldn't have completed", send2.completed());
        assertTrue("Complete the first request", producer.completeNext());
        assertFalse("Requst should be successful", send1.hasError());
        assertFalse("Second request still incomplete", send2.completed());
        IllegalArgumentException e = new IllegalArgumentException("blah");
        assertTrue("Complete the second request with an error", producer.errorNext(e));
        try {
            send2.await();
            fail("Expected error to be thrown");
        } catch (IllegalArgumentException err) {
            // this is good
        }
        assertFalse("No more requests to complete", producer.completeNext());
    }

    public void testSerializationAndPartitioning() {
        Cluster cluster = new Cluster(asList(new Node(0, "host", -1)), asList(new PartitionInfo("topic",
                                                                                                0,
                                                                                                0,
                                                                                                new int[] { 0 },
                                                                                                new int[] { 0 })));
        Serializer serializer = new StringSerialization();
        Partitioner partitioner = new DefaultPartitioner();
        MockProducer producer = new MockProducer(serializer, serializer, partitioner, cluster, true);
        ProducerRecord record = new ProducerRecord("topic", "key", "value");
        RecordSend send = producer.send(record);
        assertTrue("Send should be immediately complete", send.completed());
    }
}
