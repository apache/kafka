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
package org.apache.zookeeper.server;

import kafka.test.zk.ReceiptEvent;
import kafka.test.zk.ZkTestContext;
import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.proto.ReplyHeader;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class ZkBrokerRegistrationStubs {

    public static ServerCnxn muteConnection(ServerCnxn cnxn) {
        // Note that ServerCnxn#sendBuffer has only package-level visibility so we need
        Logger log = LoggerFactory.getLogger(ServerCnxn.class);

        ServerCnxn unresponsiveCxn = spy(cnxn);

        Answer<Void> sendResponse = invocation -> {
            ReplyHeader header = invocation.getArgument(0);
            Record record = invocation.getArgument(1);
            log.info("Dropping response " + header + " " + record);
            return null;
        };

        Answer<Void> sendBuffer = invocation -> {
            ByteBuffer buffer = invocation.getArgument(0);
            log.info("Dropping response " + buffer);
            return null;
        };

        try {
            doAnswer(sendResponse).when(unresponsiveCxn).sendResponse(any(), any(), any(), any(), any(), anyInt());
            doAnswer(sendResponse).when(unresponsiveCxn).sendResponse(any(), any(), any());
            doAnswer(sendBuffer).when(unresponsiveCxn).sendBuffer(any());
            doAnswer(i -> "(MutedCxn) " + cnxn).when(unresponsiveCxn).toString();
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        return unresponsiveCxn;
    }

    public static RequestProcessor newInstrumentedRequestProcessor(ZooKeeperServer zks, ZkTestContext testContext) {
        Logger logger = LoggerFactory.getLogger(RequestProcessor.class);
        ReceiptEvent ping = new ReceiptEvent(ZooDefs.OpCode.ping, false);
        // CloseSession is an internal request sent by the SessionTracker in the server. There is no RPC involved.
        ReceiptEvent closeSession = new ReceiptEvent(ZooDefs.OpCode.closeSession, true);

        FinalRequestProcessor delegate = new FinalRequestProcessor(zks);

        Answer<Void> processRequest = invocation -> {
            Request request = invocation.getArgument(0);

            if (testContext.isTerminated()) {
                delegate.processRequest(request);
                return null;
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

            logger.info("Processing request: " + request);

            try {
                delegate.processRequest(request);
            } finally {
                expectedReceiptEvent.serverProcessed();
            }

            return null;
        };

        FinalRequestProcessor requestProcessor = spy(delegate);
        doAnswer(processRequest).when(requestProcessor).processRequest(any());
        return requestProcessor;
    }
}
