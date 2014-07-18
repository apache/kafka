package org.apache.kafka.clients;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.producer.internals.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.MockSelector;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class NetworkClientTest {

    private MockTime time = new MockTime();
    private MockSelector selector = new MockSelector(time);
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE);
    private int nodeId = 1;
    private Cluster cluster = TestUtils.singletonCluster("test", nodeId);
    private Node node = cluster.nodes().get(0);
    private NetworkClient client = new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE, 0, 64 * 1024, 64 * 1024);

    @Before
    public void setup() {
        metadata.update(cluster, time.milliseconds());
    }

    @Test
    public void testReadyAndDisconnect() {
        List<ClientRequest> reqs = new ArrayList<ClientRequest>();
        assertFalse("Client begins unready as it has no connection.", client.ready(node, time.milliseconds()));
        assertEquals("The connection is established as a side-effect of the readiness check", 1, selector.connected().size());
        client.poll(reqs, 1, time.milliseconds());
        selector.clear();
        assertTrue("Now the client is ready", client.ready(node, time.milliseconds()));
        selector.disconnect(node.id());
        client.poll(reqs, 1, time.milliseconds());
        selector.clear();
        assertFalse("After we forced the disconnection the client is no longer ready.", client.ready(node, time.milliseconds()));
        assertTrue("Metadata should get updated.", metadata.timeToNextUpdate(time.milliseconds()) == 0);
    }

    @Test(expected = IllegalStateException.class)
    public void testSendToUnreadyNode() {
        RequestSend send = new RequestSend(5,
                                           client.nextRequestHeader(ApiKeys.METADATA),
                                           new MetadataRequest(Arrays.asList("test")).toStruct());
        ClientRequest request = new ClientRequest(time.milliseconds(), false, send, null);
        client.poll(Arrays.asList(request), 1, time.milliseconds());
    }

    @Test
    public void testSimpleRequestResponse() {
        ProduceRequest produceRequest = new ProduceRequest((short) 1, 1000, Collections.<TopicPartition, ByteBuffer>emptyMap());
        RequestHeader reqHeader = client.nextRequestHeader(ApiKeys.PRODUCE);
        RequestSend send = new RequestSend(node.id(), reqHeader, produceRequest.toStruct());
        ClientRequest request = new ClientRequest(time.milliseconds(), true, send, null);
        awaitReady(client, node);
        client.poll(Arrays.asList(request), 1, time.milliseconds());
        assertEquals(1, client.inFlightRequestCount());
        ResponseHeader respHeader = new ResponseHeader(reqHeader.correlationId());
        Struct resp = new Struct(ProtoUtils.currentResponseSchema(ApiKeys.PRODUCE.id));
        resp.set("responses", new Object[0]);
        int size = respHeader.sizeOf() + resp.sizeOf();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        respHeader.writeTo(buffer);
        resp.writeTo(buffer);
        buffer.flip();
        selector.completeReceive(new NetworkReceive(node.id(), buffer));
        List<ClientResponse> responses = client.poll(new ArrayList<ClientRequest>(), 1, time.milliseconds());
        assertEquals(1, responses.size());
        ClientResponse response = responses.get(0);
        assertTrue("Should have a response body.", response.hasResponse());
        assertEquals("Should be correlated to the original request", request, response.request());
    }

    private void awaitReady(NetworkClient client, Node node) {
        while (!client.ready(node, time.milliseconds()))
            client.poll(new ArrayList<ClientRequest>(), 1, time.milliseconds());
    }

}
