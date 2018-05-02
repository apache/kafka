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
package org.apache.kafka.clients;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.MockTime;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;


public class NetworkClientUtilsTest {
    protected final MockTime time = new MockTime();
    protected final Node node = Node.noNode();

    @Test
    public void testInterruptAwaitReady() throws Exception {
        final NetworkClient client = EasyMock.mock(NetworkClient.class);
        EasyMock.expect(client.isReady(EasyMock.anyObject(Node.class), EasyMock.anyLong())).andReturn(false).anyTimes();
        EasyMock.expect(client.ready(EasyMock.anyObject(Node.class), EasyMock.anyLong())).andReturn(false).anyTimes();
        EasyMock.expect(client.connectionFailed(EasyMock.anyObject(Node.class))).andReturn(false).anyTimes();
        EasyMock.expect(client.authenticationException(EasyMock.anyObject(Node.class))).andReturn(null).anyTimes();
        EasyMock.expect(client.poll(EasyMock.anyLong(), EasyMock.anyLong())).andReturn(null).anyTimes();
        EasyMock.replay(client);

        final AtomicBoolean canRun = new AtomicBoolean(false);
        Thread networkClientThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (!canRun.get()) { }
                    NetworkClientUtils.awaitReady(client, node, time, Integer.MAX_VALUE);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                }
                Assert.fail();
            }
        });
        networkClientThread.start();
        networkClientThread.interrupt();
        canRun.set(true);
        networkClientThread.join();
    }

    @Test
    public void testInterruptSendAndReceive() throws Exception {
        final NetworkClient client = EasyMock.mock(NetworkClient.class);
        client.send(EasyMock.anyObject(ClientRequest.class), EasyMock.anyLong());
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(client.poll(EasyMock.anyLong(), EasyMock.anyLong())).andReturn(null).anyTimes();
        EasyMock.replay(client);

        final AtomicBoolean canRun = new AtomicBoolean(false);
        Thread networkClientThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (!canRun.get()) { }
                    NetworkClientUtils.sendAndReceive(client,
                            new ClientRequest(node.idString(), null, 0, "mock",
                                    time.milliseconds(), false, null), time);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                } catch (Exception ignored) {
                    ignored.printStackTrace();
                }
                Assert.fail();
            }
        });
        networkClientThread.start();
        networkClientThread.interrupt();
        canRun.set(true);
        networkClientThread.join();
    }
}
