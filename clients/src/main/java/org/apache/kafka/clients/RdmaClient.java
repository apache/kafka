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

import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvPd;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCm;
import com.ibm.disni.verbs.RdmaCmEvent;
import com.ibm.disni.verbs.RdmaCmId;
import com.ibm.disni.verbs.RdmaEventChannel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.Optional;

/**

 * This class is not thread-safe!
 */



public abstract class RdmaClient  {


    /* pd is shared across connections to reduce memory consumption */
    // For client it can be created only after first connection is created
    // In real ibverbs it is possible to create it in advance
    protected IbvPd pd = null;


    static public class RDMAQPparams {
        public final int maxSendWr;
        public final int maxRecvWr;
        public final int maxSendSge;
        public final int maxRecvSge;
        public final int maxInlineData;


        public RDMAQPparams(int maxSendWr, int maxRecvWr) {
            this(maxSendWr, maxRecvWr, 1, 1, 0);
        }

        public RDMAQPparams(int maxSendWr, int maxRecvWr, int maxSendSge, int maxRecvSge, int maxInlineData) {
            this.maxSendWr = maxSendWr;
            this.maxRecvWr = maxRecvWr;
            this.maxSendSge = maxSendSge;
            this.maxRecvSge = maxRecvSge;
            this.maxInlineData = maxInlineData;
        }
    }


    protected final RdmaEventChannel cmChannel;


    protected final Map<Integer, SimpleVerbsEP> qpnumToConnection = new HashMap<>();
    protected final Map<String, SimpleVerbsEP> nodeToConnection = new HashMap<>();

    private final String clientId;

    private int correlation;
    private final Logger log;

    private int pendingCompletions = 0;

    private final List<ClientRDMARequest> abortedSends = new LinkedList<>();

    private final Map<Integer, Deque<PendingRequest>> pendingSendRequests = new HashMap<>(); // qpnum ->  Q

    private final Map<Integer, Deque<PendingRequest>> pendingRecvRequests = new HashMap<>(); // qpnum ->  Q


    public ClientRDMARequest newClientRdmaRequest(String destination,
                                             RDMAWrBuilder builder,
                                             long createdTimeNanos,
                                             int requestTimeoutMs,
                                             boolean expectSendCompletion,
                                             boolean expectResponse,
                                             RDMARequestCompletionHandler callback) {
        return new ClientRDMARequest(destination, correlation++, builder, clientId, createdTimeNanos, expectSendCompletion, expectResponse, callback);

    }

    public RdmaClient(String clientId,
                       LogContext logContext) throws Exception {
        this.cmChannel = RdmaEventChannel.createEventChannel();
        this.clientId = clientId;
        this.correlation = 0;
        this.log = logContext.logger(RdmaClient.class);
    }


    public boolean connect(Node node) throws Exception {
        return connect(node, Optional.empty());
    }

    public boolean connect(Node node, Optional<RDMAQPparams> cap) throws Exception {
        if (node.isEmpty())
            throw new IllegalArgumentException("Cannot connect to empty node " + node);

        return connect(node.idString(), node.host(), node.port(), cap);
    }

    public boolean connect(String id, String hostname, int port, Optional<RDMAQPparams> cap) throws Exception {


        if (nodeToConnection.containsKey(id)) {
           // System.out.println("Rdma connection already exist " + id);
            return false;
        }

        RdmaCmId idPriv = PrepareConnection(hostname, port);
        SimpleVerbsEP ep = FinalizeConnection(idPriv, cap);

        int recvSize = ep.canPostReceives;
        for (int i = 0; i < recvSize; i++) {
            ep.postEmptyRecv();
        }

        Integer qpnum = ep.getQPnum();

        qpnumToConnection.put(qpnum, ep);
        nodeToConnection.put(id, ep);


        return ep.ready();
    }


    public boolean isConnected(Node node) {
        return nodeToConnection.containsKey(node.idString());
    }


    public IbvMr MemReg(ByteBuffer buffer) throws Exception {
        if (this.pd == null) {
            throw new IOException("VerbsClient::pd null");
        }
        int access = IbvMr.IBV_ACCESS_REMOTE_READ | IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE;
        IbvMr mr = pd.regMr(buffer, access).execute().free().getMr();
        return mr;
    }

    public boolean CanRegisterMemoty() {
        return this.pd != null;
    }

    private RdmaCmId  PrepareConnection(String hostname, int port) throws Exception {

        RdmaCmId idPriv = cmChannel.createId(RdmaCm.RDMA_PS_TCP);
        if (idPriv == null) {
            throw new IOException("VerbsClient::id null");
        }

        // before connecting, we have to resolve addresses
        InetAddress hostDst = InetAddress.getByName(hostname);
        InetSocketAddress dst = new InetSocketAddress(hostDst, port);
        idPriv.resolveAddr(null, dst, 2000);

        // resolve addr returns an event, we have to catch that event
        RdmaCmEvent cmEvent = cmChannel.getCmEvent(-1);
        if (cmEvent == null) {
            throw new IOException("VerbsClient::cmEvent null");
        } else if (cmEvent.getEvent() != RdmaCmEvent.EventType.RDMA_CM_EVENT_ADDR_RESOLVED
                .ordinal()) {
            throw new IOException("VerbsClient::wrong event received: " + cmEvent.getEvent());
        }
        cmEvent.ackEvent();

        // we also have to resolve the route
        idPriv.resolveRoute(2000);
        // and catch that event too
        cmEvent = cmChannel.getCmEvent(-1);
        if (cmEvent == null) {
            throw new IOException("VerbsClient::cmEvent null");
        } else if (cmEvent.getEvent() != RdmaCmEvent.EventType.RDMA_CM_EVENT_ROUTE_RESOLVED
                .ordinal()) {
            throw new IOException("VerbsClient::wrong event received: " + cmEvent.getEvent());
        }
        cmEvent.ackEvent();
        return idPriv;
    }

    protected abstract SimpleVerbsEP FinalizeConnection(RdmaCmId idPriv, Optional<RDMAQPparams> cap) throws Exception;


    public void send(ClientRDMARequest request, long nowNanos) {
        doSend(request, nowNanos);
    }

    LinkedList<Long> times = new LinkedList<>();


    public boolean isContended(Node node) {
        String nodeId = node.idString();
        SimpleVerbsEP ep = nodeToConnection.getOrDefault(nodeId, null);
        return (ep != null) && ep.isContended();
    }

    public boolean hasInFlightRequests() {
        return pendingCompletions != 0;
    }

    private void doSend(ClientRDMARequest request, long nowNanos) {
        String nodeId = request.destination();

        //request.createdTimeNanos

        SimpleVerbsEP ep = nodeToConnection.getOrDefault(nodeId, null);
        if (ep == null) {
            abortedSends.add(request);
            return;
        }

        try {
            LinkedList<IbvSendWR> wrs = request.getWRs();


            pendingCompletions += wrs.size();

            IbvSendWR lastwr = wrs.removeLast();

            for (IbvSendWR wr : wrs) {
                int wrid = correlation++;
                wr.setWr_id(wrid);
                ep.postsend(wr, false);
            }
            int wrid = correlation++;
            lastwr.setWr_id(wrid);

            if (request.expectSendCompletion()) {
                PendingRequest pending = new PendingRequest(lastwr.getWr_id(), request, lastwr, request.createdTimeNanos);
                Deque<PendingRequest> reqs = this.pendingSendRequests.get(ep.getQPnum());
                if (reqs == null) {
                    reqs = new ArrayDeque<>();
                    this.pendingSendRequests.put(ep.getQPnum(), reqs);
                }
                reqs.addFirst(pending);
            }

            if (request.expectResponse()) {
                PendingRequest pending = new PendingRequest(lastwr.getImm_data(), request, lastwr, request.createdTimeNanos);
                Deque<PendingRequest> reqs = this.pendingRecvRequests.get(ep.getQPnum());
                if (reqs == null) {
                    reqs = new ArrayDeque<>();
                    this.pendingRecvRequests.put(ep.getQPnum(), reqs);
                }
                reqs.addFirst(pending);
            }

            times.addLast(nowNanos);
            ep.postsend(lastwr, true);

        } catch (Exception e) {
            System.out.println("Error on send");
        }
    }

    protected abstract List<IbvWC> pollwc();

    public List<ClientRDMAResponse> poll(long timeout, long nowNanos) {

        if (!abortedSends.isEmpty()) {
            List<ClientRDMAResponse> responses = new ArrayList<>();
            // handleAbortedSends(responses);
            completeResponses(responses);
            return responses;
        }

        List<IbvWC> completed = pollwc();

        try {
            for (Map.Entry<Integer, SimpleVerbsEP> entry : qpnumToConnection.entrySet()) {
                SimpleVerbsEP ep = entry.getValue();
                if (ep.hasUnsentRequests()) {
                    ep.trigger_send();
                }
            }
        } catch (Exception e) {
            System.out.println("Error on trigger_send");
        }

        List<ClientRDMAResponse> responses = new ArrayList<>();
        try {
            handleWC(responses, completed, nowNanos);
        } catch (Exception e) {
            System.out.println("Error on handleWC" + e.toString());
        }


        completeResponses(responses);
        return responses;
    }

    //private long last_poll_time = 0L;

    private void handleWC(List<ClientRDMAResponse> responses,  List<IbvWC> wcs, long nowNanos) throws Exception {
        // if no response is expected then when the send is completed, return it
        for (IbvWC wc : wcs) {
            pendingCompletions--;

            boolean isRecv = wc.getOpcode() >= IbvWC.IbvWcOpcode.IBV_WC_RECV.getOpcode();

            int qpnum = wc.getQp_num();

            if (isRecv)
                qpnumToConnection.get(qpnum).postEmptyRecv();
            else {
            /*     long sendingtime = nowNanos - times.pollFirst();
                long pollGap = nowNanos-last_poll_time;
                if (sendingtime > 1_000_000 && pollGap > 1_000_000 ) { // 1 ms
                    System.out.println("sendingtime is "+sendingtime + "Last poll was "+ pollGap);
                }*/
            }

            Deque<PendingRequest> reqs = isRecv ? this.pendingRecvRequests.get(qpnum) :
                                                  this.pendingSendRequests.get(qpnum);

            if (reqs == null) {
                continue;
            }

            PendingRequest request = reqs.getLast();

            if (isRecv) {
                assert request.tomatch == wc.getImm_data();
            } else {
                if (request.tomatch != wc.getWr_id())
                    continue;
            }

            reqs.pollLast();
            responses.add(request.completed(wc, nowNanos));
        }
 //       last_poll_time = nowNanos;
    }


    private void completeResponses(List<ClientRDMAResponse> responses) {
        for (ClientRDMAResponse response : responses) {
            try {
                response.onComplete();
            } catch (Exception e) {
                log.error("Uncaught error in request completion:", e);
            }
        }
    }


    static class PendingRequest {
        final long tomatch;
        final String destination;
        final RDMARequestCompletionHandler callback;
        final boolean expectResponse;
        final ClientRDMARequest request;
        final IbvSendWR wr;
        final long createdTimeNanos;

        public PendingRequest(long tomatch, ClientRDMARequest request, IbvSendWR wr, long now) {
            this(tomatch, request.destination(), request.callback(), request.expectResponse(), request, wr, now);
        }

        public PendingRequest(long tomatch,
                               String destination,
                               RDMARequestCompletionHandler callback,
                               boolean expectResponse,
                               ClientRDMARequest request,
                               IbvSendWR wr,
                               long createdTimeNanos) {
            this.tomatch = tomatch;
            this.destination = destination;
            this.callback = callback;
            this.expectResponse = expectResponse;
            this.request = request;
            this.wr = wr;
            this.createdTimeNanos = createdTimeNanos;
        }

        public ClientRDMAResponse completed(IbvWC wc, long timeNanos) {
            return new ClientRDMAResponse(request.getBuilder(), callback, destination, request.getBuffer(), wc, createdTimeNanos, timeNanos);
        }

        @Override
        public String toString() {
            return "PendingRequest(tomatch=" + tomatch +
                    ", destination=" + destination +
                    ", expectResponse=" + expectResponse +
                    ", request=" + request +
                    ", callback=" + callback +
                    ", wr=" + wr + ")";
        }
    }



}
