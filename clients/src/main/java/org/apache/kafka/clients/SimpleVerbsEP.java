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
import com.ibm.disni.verbs.IbvQP;
import com.ibm.disni.verbs.IbvCQ;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvRecvWR;
import com.ibm.disni.verbs.SVCPollCq;
import com.ibm.disni.verbs.SVCPostRecv;

import java.util.LinkedList;
import java.util.List;

public class SimpleVerbsEP {

    private final RdmaCmId id;
    private final IbvQP qp;
    private final IbvCQ sendcq;
    private final IbvCQ recvcq;


    public final int canPostReceives;

    private int canIssueRdma;
    private int canSendRequests;

    private LinkedList<IbvSendWR> delayedWrites = new LinkedList<IbvSendWR>();


    private final IbvWC[] sendwcList;
    private final IbvWC[] recvwcList;


    SVCPollCq sendpoll = null;
    SVCPollCq recvpoll = null;

    SVCPostRecv emptyrecv = null;

    private final  int contendedLimit;

    SimpleVerbsEP(RdmaCmId id, IbvQP qp, IbvCQ sendcq, IbvCQ recvcq, int maxRecvSize,
                  int maxSendSize, int requestQuota, int wcBatch, int contendedLimit) {
        this.id = id;
        this.qp = qp;
        this.sendcq = sendcq;
        this.recvcq = recvcq;
        this.canIssueRdma = maxSendSize;
        this.canSendRequests = requestQuota;
        this.canPostReceives = maxRecvSize;
        sendwcList = new IbvWC[wcBatch];
        recvwcList = new IbvWC[wcBatch];
        for (int i = 0; i < sendwcList.length; i++) {
            sendwcList[i] = new IbvWC();
        }
        for (int i = 0; i < recvwcList.length; i++) {
            recvwcList[i] = new IbvWC();
        }
        this.contendedLimit = contendedLimit;

    }

    public boolean ready() {
        return true; }

    public int getQPnum() throws Exception {
        return qp.getQp_num(); }

    public void postsend(IbvSendWR wr, boolean dosend) throws Exception {
        delayedWrites.add(wr);

        if (dosend) {
            trigger_send();
        }
    }

    void trigger_send() throws Exception {

        int canSend = Math.min(canIssueRdma, canSendRequests);
        if (canSend == 0)
            return;

        int willSend = Math.min(canSend, delayedWrites.size());

        if (willSend == 0)
            return;


        LinkedList<IbvSendWR> wrListToSend =  new LinkedList<>();

        for (int i = 0; i < willSend; i++) {
            wrListToSend.add(delayedWrites.pollFirst());
        }

        canIssueRdma -= willSend;
        canSendRequests -= willSend;

        qp.postSend(wrListToSend, null).execute().free();
    }



    /*
        We use stateful poll request to reduce overhead of polling.
        Warning! IbvWC must be processed before polling again!

     */
    public LinkedList<IbvWC> pollsend() throws Exception {
        LinkedList<IbvWC> recvList = new LinkedList<>();
        pollsend(recvList);
        return recvList;
    }

    public int pollsend(LinkedList<IbvWC> recvList) throws Exception {
        if (this.sendpoll == null) {
            this.sendpoll = sendcq.poll(sendwcList, sendwcList.length);
        }
        int compl = this.sendpoll.execute().getPolls();
        for (int i = 0; i < compl; i++) {
            if (sendwcList[i].getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RDMA_READ.getOpcode()) {
                canSendRequests++;
            }
            recvList.add(sendwcList[i]);
            canIssueRdma++;
        }

        return compl;
    }

    public boolean hasUnsentRequests() {
        return !delayedWrites.isEmpty();
    }

    public boolean isContended() {
        return delayedWrites.size() > this.contendedLimit;
    }



    public LinkedList<IbvWC> pollrecv() throws Exception {
        LinkedList<IbvWC> recvList = new LinkedList<>();
        pollrecv(recvList);
        return recvList;
    }

    public int pollrecv(LinkedList<IbvWC> recvList) throws Exception {
        if (this.recvpoll == null) {
            this.recvpoll = recvcq.poll(recvwcList, sendwcList.length);
        }
        int compl = this.recvpoll.execute().getPolls();
        for (int i = 0; i < compl; i++) {
            recvList.add(recvwcList[i]);
            canSendRequests++;
        }
        return compl;
    }



    public void postEmptyRecv() throws Exception {
        if (emptyrecv == null) {
            IbvRecvWR wr = new IbvRecvWR();
            List<IbvRecvWR>  listWR = new LinkedList<>();
            listWR.add(wr);
            emptyrecv = qp.postRecv(listWR, null);
        }

        emptyrecv.execute();
    }
}
