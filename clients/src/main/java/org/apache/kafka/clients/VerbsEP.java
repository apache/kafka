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

public class VerbsEP {

    private final RdmaCmId id;
    private final IbvQP qp;
    private final IbvCQ sendcq;
    private final IbvCQ recvcq;

    private int canPostSends;
    private volatile Integer canPostSendSignaled;
    private volatile Integer canPostReceives;


    private LinkedList<IbvSendWR> wrListSend = new LinkedList<IbvSendWR>();


    private int signaledcounter = 1;
    private LinkedList<Integer> signaledafter = new LinkedList<>();

    IbvWC[] sendwcList = new IbvWC[8];
    IbvWC[] recvwcList = new IbvWC[8];
    SVCPollCq sendpoll = null;
    SVCPollCq recvpoll = null;

    SVCPostRecv emptyrecv = null;

    VerbsEP(RdmaCmId id, IbvQP qp, IbvCQ sendcq, IbvCQ recvcq,
             int canPostSends, Integer canPostSendSignaled, Integer canPostReceives) {
        this.id = id;
        this.qp = qp;
        this.sendcq = sendcq;
        this.recvcq = recvcq;
        this.canPostSends = canPostSends;
        this.canPostSendSignaled = canPostSendSignaled;
        this.canPostReceives = canPostReceives;

        for (int i = 0; i < sendwcList.length; i++) {
            sendwcList[i] = new IbvWC();
        }
        for (int i = 0; i < recvwcList.length; i++) {
            recvwcList[i] = new IbvWC();
        }
    }

    public boolean ready() {
        return true;
    }

    public int getQPnum() throws Exception {
        return qp.getQp_num();
    }

    public void postsend(IbvSendWR wr, boolean dosend) throws Exception {

        wrListSend.add(wr);

        if (canPostSends == 0) {
            return;
        }

        if (dosend) {
            trigger_send();
        }
    }

    private void trigger_send() throws Exception {
        LinkedList<IbvSendWR> wrListToSend = new LinkedList<IbvSendWR>();

        while (!wrListSend.isEmpty() && canPostSends > 0) {
            if (wrListSend.getFirst().getSend_flags() == IbvSendWR.IBV_SEND_SIGNALED || signaledcounter > 10) {
                if (canPostSendSignaled > 0) {
                    wrListSend.getFirst().setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
                    canPostSendSignaled--;
                    signaledafter.add(signaledcounter);
                    signaledcounter = 0;
                } else {
                    break;
                }
            }

            wrListToSend.add(wrListSend.pollFirst());

            signaledcounter++;
            canPostSends--;
        }

        if (wrListToSend.isEmpty()) {
            return;
        }

        qp.postSend(wrListToSend, null).execute().free();
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

    /*
        We use stateful poll request to reduce overhead of polling.
        Warning! IbvWC must be processed before polling again!
     */
    public LinkedList<IbvWC> pollsend() throws Exception {
        if (this.sendpoll == null) {
            this.sendpoll = sendcq.poll(sendwcList, sendwcList.length);
        }
        LinkedList<IbvWC> recvList = new LinkedList<>();
        int compl = this.sendpoll.execute().getPolls();
        for (int i = 0; i < compl; i++) {
            recvList.add(sendwcList[i]);
            canPostSends += signaledafter.pollFirst();
            canPostSendSignaled++;
        }

        if (compl != 0) {
            trigger_send();
        }

        return recvList;
    }

    public LinkedList<IbvWC> pollrecv() throws Exception {
        if (this.recvpoll == null) {
            this.recvpoll = recvcq.poll(recvwcList, sendwcList.length);
        }
        LinkedList<IbvWC> recvList =  new LinkedList<>();
        int compl = this.recvpoll.execute().getPolls();
        for (int i = 0; i < compl; i++) {
            recvList.add(recvwcList[i]);
        }
        return recvList;
    }
}
