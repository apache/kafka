package kafka.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import kafka.common.network.NetworkReceive;
import kafka.common.network.NetworkSend;
import kafka.common.network.Selectable;
import kafka.common.utils.Time;

/**
 * A fake selector to use for testing
 */
public class MockSelector implements Selectable {

    private final Time time;
    private final List<NetworkSend> completedSends = new ArrayList<NetworkSend>();
    private final List<NetworkReceive> completedReceives = new ArrayList<NetworkReceive>();
    private final List<Integer> disconnected = new ArrayList<Integer>();
    private final List<Integer> connected = new ArrayList<Integer>();

    public MockSelector(Time time) {
        this.time = time;
    }

    @Override
    public void connect(int id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        this.connected.add(id);
    }

    @Override
    public void disconnect(int id) {
        this.disconnected.add(id);
    }

    @Override
    public void wakeup() {
    }

    @Override
    public void close() {
    }

    public void clear() {
        this.completedSends.clear();
        this.completedReceives.clear();
        this.disconnected.clear();
        this.connected.clear();
    }

    @Override
    public void poll(long timeout, List<NetworkSend> sends) throws IOException {
        this.completedSends.addAll(sends);
        time.sleep(timeout);
    }

    @Override
    public List<NetworkSend> completedSends() {
        return completedSends;
    }

    public void completeSend(NetworkSend send) {
        this.completedSends.add(send);
    }

    @Override
    public List<NetworkReceive> completedReceives() {
        return completedReceives;
    }

    public void completeReceive(NetworkReceive receive) {
        this.completedReceives.add(receive);
    }

    @Override
    public List<Integer> disconnected() {
        return disconnected;
    }

    @Override
    public List<Integer> connected() {
        return connected;
    }

}
