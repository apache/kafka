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
package kafka.test.zk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.zookeeper.server.ClientCnxnLimitException;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.SessionTracker;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.util.JvmPauseMonitor;

import static org.apache.zookeeper.server.ZkBrokerRegistrationStubs.newInstrumentedRequestProcessor;

/**
 * A specialization of the ZooKeeperServer which allows to intercept new connection
 * and session expiration in order to validate they correspond to the expected test
 * timeline, and also to simulate network or processing delays.
 */
public class InstrumentedZooKeeperServer extends ZooKeeperServer {
    private final ZkTestContext testContext;
    private final Set<Long> sessionIds = new ConcurrentSkipListSet<>();

    public InstrumentedZooKeeperServer(
        JvmPauseMonitor jvmPauseMonitor,
        FileTxnSnapLog txnLogFactory,
        int tickTime,
        int minSessionTimeout,
        int maxSessionTimeout,
        int clientPortListenBacklog,
        ZKDatabase zkDb,
        String initialConfig,
        ZkTestContext testContext
    ) {
        super(
            jvmPauseMonitor,
            txnLogFactory,
            tickTime,
            minSessionTimeout,
            maxSessionTimeout,
            clientPortListenBacklog,
            zkDb,
            initialConfig
        );
        this.testContext = testContext;
    }

    @Override
    protected void setupRequestProcessors() {
        RequestProcessor processor = newInstrumentedRequestProcessor(this, testContext);
        RequestProcessor syncProcessor = new SyncRequestProcessor(this, processor);
        ((SyncRequestProcessor) syncProcessor).start();
        firstProcessor = new PrepRequestProcessor(this, syncProcessor);
        ((PrepRequestProcessor) firstProcessor).start();
    }

    @Override
    public void processConnectRequest(ServerCnxn cnxn, ByteBuffer incomingBuffer) throws IOException, ClientCnxnLimitException {
        if (!testContext.isTerminated()) {
            testContext.connectionTimeline.next().maybeInjectDelay();
        }
        super.processConnectRequest(cnxn, incomingBuffer);
        long sessionId = cnxn.getSessionId();
        if (sessionId != 0) {
            sessionIds.add(sessionId);
        }
    }

    @Override
    public void expire(SessionTracker.Session session) {
        // Check if sessionIds contains the session to avoid including sessions created outside the
        // test in the sequence of session expiration events.
        if (!testContext.isTerminated() && sessionIds.contains(session.getSessionId())) {
            testContext.sessionExpirationTimeline.next().maybeInjectDelay();
        }
        super.expire(session);
    }
}
