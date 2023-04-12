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
package kafka.zk;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A specialization of Zookeeper FinalRequestProcessor which processes transactions in Zookeeper.
 * This allows to intercept the requests processed by Zookeeper and perform any action required
 * for the orchestration of the test and injection of failures.
 */
public class InstrumentedRequestProcessor extends FinalRequestProcessor {
    private final Logger log = LoggerFactory.getLogger(InstrumentedRequestProcessor.class);

    private final ReceiptEvent ping = new ReceiptEvent(ZooDefs.OpCode.ping, false);
    // CloseSession is an internal request sent by the SessionTracker in the server. There is no RPC involved.
    private final ReceiptEvent closeSession = new ReceiptEvent(ZooDefs.OpCode.closeSession, true);

    private final ZkTestContext testContext;

    public InstrumentedRequestProcessor(ZooKeeperServer zks, ZkTestContext testContext) {
        super(zks);
        this.testContext = testContext;
    }

    @Override
    public void processRequest(Request request) {
        if (testContext.isTerminated()) {
            super.processRequest(request);
            return;
        }

        ReceiptEvent expectedReceiptEvent;

        if (request.type == ZooDefs.OpCode.ping) {
            expectedReceiptEvent = ping;
        } else if (request.type == ZooDefs.OpCode.closeSession) {
            expectedReceiptEvent = closeSession;
        } else {
            if (!testContext.requestTimelime.hasNext()) {
                throw new AssertionError(request);
            }

            expectedReceiptEvent = testContext.requestTimelime.next();
        }


        if (!expectedReceiptEvent.matches(request)) {
            throw new AssertionError("Expected: " + expectedReceiptEvent + " Actual: " + request);
        }

        request = expectedReceiptEvent.maybeDecorate(request);

        log.info("Processing request: " + request);
        expectedReceiptEvent.serverReceived();

        try {
            super.processRequest(request);
        } finally {
            expectedReceiptEvent.serverProcessed();
        }
    }
}
