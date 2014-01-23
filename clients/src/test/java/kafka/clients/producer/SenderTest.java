package kafka.clients.producer;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import kafka.clients.producer.internals.Metadata;
import kafka.clients.producer.internals.RecordAccumulator;
import kafka.clients.producer.internals.Sender;
import kafka.common.Cluster;
import kafka.common.Node;
import kafka.common.PartitionInfo;
import kafka.common.TopicPartition;
import kafka.common.metrics.Metrics;
import kafka.common.network.NetworkReceive;
import kafka.common.protocol.ApiKeys;
import kafka.common.protocol.Errors;
import kafka.common.protocol.ProtoUtils;
import kafka.common.protocol.types.Struct;
import kafka.common.record.CompressionType;
import kafka.common.requests.RequestSend;
import kafka.common.requests.ResponseHeader;
import kafka.common.utils.MockTime;
import kafka.test.MockSelector;

import org.junit.Before;
import org.junit.Test;

public class SenderTest {

    private MockTime time = new MockTime();
    private MockSelector selector = new MockSelector(time);
    private int batchSize = 16 * 1024;
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE);
    private Cluster cluster = new Cluster(asList(new Node(0, "localhost", 1969)), asList(new PartitionInfo("test",
                                                                                                           0,
                                                                                                           0,
                                                                                                           new int[0],
                                                                                                           new int[0])));
    private Metrics metrics = new Metrics(time);
    private RecordAccumulator accumulator = new RecordAccumulator(batchSize, 1024 * 1024, 0L, false, metrics, time);
    private Sender sender = new Sender(selector, metadata, this.accumulator, "", 1024 * 1024, 0L, (short) -1, 10000, time);

    @Before
    public void setup() {
        metadata.update(cluster, time.milliseconds());
    }

    @Test
    public void testSimple() throws Exception {
        TopicPartition tp = new TopicPartition("test", 0);
        RecordSend send = accumulator.append(tp, "key".getBytes(), "value".getBytes(), CompressionType.NONE, null);
        sender.run(time.milliseconds());
        assertEquals("We should have connected", 1, selector.connected().size());
        selector.clear();
        sender.run(time.milliseconds());
        assertEquals("Single request should be sent", 1, selector.completedSends().size());
        RequestSend request = (RequestSend) selector.completedSends().get(0);
        selector.clear();
        long offset = 42;
        selector.completeReceive(produceResponse(request.header().correlationId(),
                                                 cluster.leaderFor(tp).id(),
                                                 tp.topic(),
                                                 tp.partition(),
                                                 offset,
                                                 Errors.NONE.code()));
        sender.run(time.milliseconds());
        assertTrue("Request should be completed", send.completed());
        assertEquals(offset, send.offset());
    }

    private NetworkReceive produceResponse(int correlation, int source, String topic, int part, long offset, int error) {
        Struct struct = new Struct(ProtoUtils.currentResponseSchema(ApiKeys.PRODUCE.id));
        Struct response = struct.instance("responses");
        response.set("topic", topic);
        Struct partResp = response.instance("partition_responses");
        partResp.set("partition", part);
        partResp.set("error_code", (short) error);
        partResp.set("base_offset", offset);
        response.set("partition_responses", new Object[] { partResp });
        struct.set("responses", new Object[] { response });
        ResponseHeader header = new ResponseHeader(correlation);
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + struct.sizeOf());
        header.writeTo(buffer);
        struct.writeTo(buffer);
        buffer.rewind();
        return new NetworkReceive(source, buffer);
    }

}
