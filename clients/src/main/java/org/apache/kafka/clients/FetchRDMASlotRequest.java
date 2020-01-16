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

import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvSendWR.Rdma;
import com.ibm.disni.verbs.IbvSge;

import java.nio.ByteBuffer;
import java.util.LinkedList;


public class FetchRDMASlotRequest implements RDMAWrBuilder {

    private long remoteAddress;
    private int rkey;
    private int length;

    private ByteBuffer targetBuffer;
    private int lkey;

    public FetchRDMASlotRequest(long remoteAddress, int rkey, int length, ByteBuffer targetBuffer, int lkey) {
        this.remoteAddress = remoteAddress;
        this.rkey = rkey;
        this.length = length;
        this.targetBuffer = targetBuffer;
        this.lkey = lkey;
    }

    @Override
    public LinkedList<IbvSendWR> build() {
        LinkedList<IbvSendWR> wrs = new LinkedList<>();

        IbvSge sgeSend = new IbvSge();
        sgeSend.setAddr(((sun.nio.ch.DirectBuffer) targetBuffer).address());
        sgeSend.setLength(length);
        sgeSend.setLkey(lkey);
        LinkedList<IbvSge> sgeList = new LinkedList<>();
        sgeList.add(sgeSend);


        IbvSendWR sendWR = new IbvSendWR();
        //sendWR.setWr_id(1002);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);


        Rdma rdmapart = sendWR.getRdma();
        rdmapart.setRemote_addr(remoteAddress);
        rdmapart.setRkey(rkey);

        wrs.add(sendWR);

        return wrs;
    }

    @Override
    public ByteBuffer getTargetBuffer() {
        return targetBuffer;
    }
}
