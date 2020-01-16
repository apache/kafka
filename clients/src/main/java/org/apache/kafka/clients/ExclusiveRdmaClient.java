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


import com.ibm.disni.verbs.RdmaCmId;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.IbvContext;
import com.ibm.disni.verbs.IbvCompChannel;
import com.ibm.disni.verbs.IbvCQ;
import com.ibm.disni.verbs.IbvQPInitAttr;
import com.ibm.disni.verbs.IbvQP;
import com.ibm.disni.verbs.RdmaConnParam;
import com.ibm.disni.verbs.RdmaCmEvent;
import org.apache.kafka.common.utils.LogContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class ExclusiveRdmaClient extends RdmaClient {


    final int wcBatch;
    final int requestQuota;
    final int completionQsize;
    final int contendedLimit;
    protected final RDMAQPparams defaultConParams;

    public ExclusiveRdmaClient(String clientId,
                               LogContext logContext, RDMAQPparams defaultParams, int requestQuota, int completionQsize, int wcBatch, int contendedLimit) throws Exception {
        super(clientId, logContext);
        if (defaultParams == null) {
            this.defaultConParams = new RDMAQPparams(10, 10);
        } else {
            this.defaultConParams = defaultParams;
        }
        this.completionQsize = completionQsize;
        this.requestQuota = requestQuota;
        this.wcBatch = wcBatch;
        this.contendedLimit = contendedLimit;
    }

    @Override
    protected SimpleVerbsEP FinalizeConnection(RdmaCmId idPriv, Optional<RDMAQPparams> optCap) throws Exception {

        RDMAQPparams cap = optCap.orElse(this.defaultConParams);

        // let's create a device context
        IbvContext context = idPriv.getVerbs();

        if (pd == null) {
            this.pd = context.allocPd();
            if (this.pd == null) {
                throw new IOException("VerbsClient::pd null");
            }
        }

        // the comp channel is used for getting CQ events
        IbvCompChannel sendcompChannel = context.createCompChannel();
        if (sendcompChannel == null) {
            throw new IOException("VerbsClient::compChannel null");
        }

        // let's create a completion queue
        IbvCQ sendcq = context.createCQ(sendcompChannel, completionQsize, 0);
        if (sendcq == null) {
            throw new IOException("VerbsClient::cq null");
        }

        IbvCompChannel recvcompChannel = context.createCompChannel();
        if (recvcompChannel == null) {
            throw new IOException("VerbsClient::compChannel null");
        }

        // let's create a completion queue
        IbvCQ recvcq = context.createCQ(recvcompChannel, completionQsize, 0);
        if (recvcq == null) {
            throw new IOException("VerbsClient::cq null");
        }

        // and request to be notified for this queue
        // cq.reqNotification(false).execute().free();

        // we prepare for the creation of a queue pair (QP)
        IbvQPInitAttr attr = new IbvQPInitAttr();
        attr.cap().setMax_recv_sge(cap.maxRecvSge);
        attr.cap().setMax_recv_wr(cap.maxRecvWr);
        attr.cap().setMax_send_sge(cap.maxSendSge);
        attr.cap().setMax_send_wr(cap.maxSendWr);
        attr.setQp_type(IbvQP.IBV_QPT_RC);
        attr.setRecv_cq(recvcq);
        attr.setSend_cq(sendcq);
        // let's create a queue pair
        IbvQP qp = idPriv.createQP(pd, attr);
        if (qp == null) {
            throw new IOException("VerbsClient::qp null");
        }

        // now let's connect to the server
        RdmaConnParam connParam = new RdmaConnParam();
        connParam.setRetry_count((byte) 2);
        idPriv.connect(connParam);

        // wait until we are really connected
        RdmaCmEvent cmEvent = cmChannel.getCmEvent(-1);
        if (cmEvent == null) {
            throw new IOException("VerbsClient::cmEvent null");
        } else if (cmEvent.getEvent() != RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED
                .ordinal()) {
            throw new IOException("VerbsClient::wrong event received: " + cmEvent.getEvent());
        }
        cmEvent.ackEvent();
        System.out.println("Connected rdma");
        return new SimpleVerbsEP(idPriv, qp, sendcq, recvcq, cap.maxRecvWr, cap.maxSendWr, this.requestQuota, this.wcBatch, this.contendedLimit);
    }


    @Override
    protected List<IbvWC> pollwc() {
        LinkedList<IbvWC> wcs = new LinkedList<IbvWC>();

        try {
            for (Map.Entry<Integer, SimpleVerbsEP> entry : qpnumToConnection.entrySet()) {
                SimpleVerbsEP ep = entry.getValue();
                int completedSends = ep.pollsend(wcs);
                int completedRecvs = ep.pollrecv(wcs);
                if (completedSends + completedRecvs > 0) {
                    ep.trigger_send();
                }
            }
        } catch (Exception e) {
            System.out.println("Uncaught error in request completion: " + e);
        }

        return wcs;
    }
}
