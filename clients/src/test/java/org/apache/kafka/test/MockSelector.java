/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.test;

import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.requests.ByteBufferChannel;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A fake selector to use for testing
 */
public class MockSelector implements Selectable {

    private final Time time;
    private final List<Send> initiatedSends = new ArrayList<>();
    private final List<Send> completedSends = new ArrayList<>();
    private final List<NetworkReceive> completedReceives = new ArrayList<>();
    private final Map<String, ChannelState> disconnected = new HashMap<>();
    private final List<String> connected = new ArrayList<>();
    private final List<DelayedReceive> delayedReceives = new ArrayList<>();

    public MockSelector(Time time) {
        this.time = time;
    }

    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        this.connected.add(id);
    }

    @Override
    public void wakeup() {
    }

    @Override
    public void close() {
    }

    @Override
    public void close(String id) {
        // Note that there are no notifications for client-side disconnects

        removeSendsForNode(id, completedSends);
        removeSendsForNode(id, initiatedSends);

        for (int i = 0; i < this.connected.size(); i++) {
            if (this.connected.get(i).equals(id)) {
                this.connected.remove(i);
                break;
            }
        }
    }

    /**
     * Simulate a server disconnect. This id will be present in {@link #disconnected()} on
     * the next {@link #poll(long)}.
     */
    public void serverDisconnect(String id) {
        this.disconnected.put(id, ChannelState.READY);
        close(id);
    }

    private void removeSendsForNode(String id, Collection<Send> sends) {
        Iterator<Send> iter = sends.iterator();
        while (iter.hasNext()) {
            Send send = iter.next();
            if (id.equals(send.destination()))
                iter.remove();
        }
    }

    public void clear() {
        this.completedSends.clear();
        this.completedReceives.clear();
        this.disconnected.clear();
        this.connected.clear();
    }

    @Override
    public void send(Send send) {
        this.initiatedSends.add(send);
    }

    @Override
    public void poll(long timeout) throws IOException {
        completeInitiatedSends();
        completeDelayedReceives();
        time.sleep(timeout);
    }

    private void completeInitiatedSends() throws IOException {
        for (Send send : initiatedSends) {
            completeSend(send);
        }
        this.initiatedSends.clear();
    }

    private void completeSend(Send send) throws IOException {
        // Consume the send so that we will be able to send more requests to the destination
        try (ByteBufferChannel discardChannel = new ByteBufferChannel(send.size())) {
            while (!send.completed()) {
                send.writeTo(discardChannel);
            }
            completedSends.add(send);
        }
    }

    private void completeDelayedReceives() {
        for (Send completedSend : completedSends) {
            Iterator<DelayedReceive> delayedReceiveIterator = delayedReceives.iterator();
            while (delayedReceiveIterator.hasNext()) {
                DelayedReceive delayedReceive = delayedReceiveIterator.next();
                if (delayedReceive.source().equals(completedSend.destination())) {
                    completedReceives.add(delayedReceive.receive());
                    delayedReceiveIterator.remove();
                }
            }
        }
    }

    @Override
    public List<Send> completedSends() {
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

    public void delayedReceive(DelayedReceive receive) {
        this.delayedReceives.add(receive);
    }

    @Override
    public Map<String, ChannelState> disconnected() {
        return disconnected;
    }

    @Override
    public List<String> connected() {
        List<String> currentConnected = new ArrayList<>(connected);
        connected.clear();
        return currentConnected;
    }

    @Override
    public void mute(String id) {
    }

    @Override
    public void unmute(String id) {
    }

    @Override
    public void muteAll() {
    }

    @Override
    public void unmuteAll() {
    }

    @Override
    public boolean isChannelReady(String id) {
        return true;
    }

    public void reset() {
        clear();
        initiatedSends.clear();
        delayedReceives.clear();
    }
}
