/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.utils.Time;

/**
 * A fake selector to use for testing
 */
public class MockSelector implements Selectable {

    private final Time time;
    private final List<Send> initiatedSends = new ArrayList<Send>();
    private final List<Send> completedSends = new ArrayList<Send>();
    private final List<NetworkReceive> completedReceives = new ArrayList<NetworkReceive>();
    private final List<String> disconnected = new ArrayList<String>();
    private final List<String> connected = new ArrayList<String>();

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
        this.disconnected.add(id);
        for (int i = 0; i < this.connected.size(); i++) {
            if (this.connected.get(i).equals(id)) {
                this.connected.remove(i);
                break;
            }
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
        this.completedSends.addAll(this.initiatedSends);
        this.initiatedSends.clear();
        time.sleep(timeout);
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

    @Override
    public List<String> disconnected() {
        return disconnected;
    }

    @Override
    public List<String> connected() {
        return connected;
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
}
