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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.util.List;

public class NetworkClientUtils {
    private final KafkaClient client;
    private final Time time;
    private boolean wakeup = false;

    public NetworkClientUtils(
            final Time time,
            final KafkaClient client) {
        this.time = time;
        this.client = client;
    }

    public boolean trySend(ClientRequest request, Node node) {
        long now = time.milliseconds();
        if (client.ready(node, now)) {
            client.send(request, now);
            return true;
        }
        // the node is not ready
        return false;
    }

    public List<ClientResponse> poll(Timer timer, boolean disableWakeup) {
        if (!disableWakeup) {
            // trigger wakeups after checking for disconnects so that the callbacks will be ready
            // to be fired on the next call to poll()
            maybeTriggerWakeup();
        }
        return this.client.poll(timer.timeoutMs(), time.milliseconds());
    }

    public List<ClientResponse> poll() {
        return this.poll(time.timer(0), false);
    }

    public void maybeTriggerWakeup() {
        if (wakeup) {
            wakeup = false;
            throw new WakeupException();
        }
    }

    public void wakeup() {
        this.wakeup = true;
        this.client.wakeup();
    }

    public Node leastLoadedNode() {
        return this.client.leastLoadedNode(time.milliseconds());
    }
}
