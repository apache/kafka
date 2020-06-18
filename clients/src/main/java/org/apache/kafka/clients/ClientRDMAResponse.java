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

import com.ibm.disni.verbs.IbvWC;

import java.nio.ByteBuffer;

/**
 * A response from the server. Contains both the body of the response as well as the correlated request
 * metadata that was originally sent.
 */
public class ClientRDMAResponse {

    private final RDMARequestCompletionHandler callback;
    private final String destination;
   // private final IbvWC wc;
    private final ByteBuffer responseBody;
    private final int bodylen;

    private final RDMAWrBuilder request;

    public final long receivedTimeMs;
    private final long latencyNanos;

    public ClientRDMAResponse(RDMAWrBuilder request, RDMARequestCompletionHandler callback,
                              String destination, ByteBuffer responseBody,
                              IbvWC wc,  long createdTimeNanos,
                              long receivedTimeNanos) {
        this.request = request;
        this.callback = callback;
        this.destination = destination;
        //this.wc = wc;
        this.responseBody = responseBody;
        this.bodylen = wc.getByte_len();
        this.receivedTimeMs = receivedTimeNanos;
        this.latencyNanos = receivedTimeNanos - createdTimeNanos;
    }


    public RDMAWrBuilder getRequest() {
        return request;
    }

    public String destination() {
        return destination;
    }


    public ByteBuffer responseBody() {
        return responseBody;
    }

    public boolean hasResponse() {
        return responseBody != null;
    }

    public int GetLength() {
        return bodylen;
    }

    public void onComplete() {
        if (callback != null)
            callback.onComplete(this);
    }

    public long requestLatencyNanos() {
        return latencyNanos;
    }

    @Override
    public String toString() {
        return "ClientRDMAResponse(responseBody=" +
               responseBody +
               ")";
    }

}
