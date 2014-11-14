package org.apache.kafka.clients;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Time;

/**
 * A mock network client for use testing code
 */
public class MockClient implements KafkaClient {

    private final Time time;
    private int correlation = 0;
    private final Set<Integer> ready = new HashSet<Integer>();
    private final Queue<ClientRequest> requests = new ArrayDeque<ClientRequest>();
    private final Queue<ClientResponse> responses = new ArrayDeque<ClientResponse>();

    public MockClient(Time time) {
        this.time = time;
    }

    @Override
    public boolean isReady(Node node, long now) {
        return ready.contains(node.id());
    }

    @Override
    public boolean ready(Node node, long now) {
        boolean found = isReady(node, now);
        ready.add(node.id());
        return found;
    }

    @Override
    public long connectionDelay(Node node, long now) {
        return 0;
    }

    public void disconnect(Integer node) {
        Iterator<ClientRequest> iter = requests.iterator();
        while (iter.hasNext()) {
            ClientRequest request = iter.next();
            if (request.request().destination() == node) {
                responses.add(new ClientResponse(request, time.milliseconds(), true, null));
                iter.remove();
            }
        }
        ready.remove(node);
    }

    @Override
    public List<ClientResponse> poll(List<ClientRequest> requests, long timeoutMs, long now) {
        this.requests.addAll(requests);
        List<ClientResponse> copy = new ArrayList<ClientResponse>(this.responses);
        this.responses.clear();
        return copy;
    }

    public Queue<ClientRequest> requests() {
        return this.requests;
    }

    public void respond(Struct body) {
        ClientRequest request = requests.remove();
        responses.add(new ClientResponse(request, time.milliseconds(), false, body));
    }

    @Override
    public int inFlightRequestCount() {
        return requests.size();
    }

    @Override
    public RequestHeader nextRequestHeader(ApiKeys key) {
        return new RequestHeader(key.id, "mock", correlation++);
    }

    @Override
    public void wakeup() {
    }

    @Override
    public void close() {
    }

    @Override
    public Node leastLoadedNode(long now) {
        return null;
    }

}
