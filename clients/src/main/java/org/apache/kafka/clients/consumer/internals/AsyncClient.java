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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public abstract class AsyncClient<T1, Req extends AbstractRequest, Resp extends AbstractResponse, T2> {

    private final Logger log;
    private final ConsumerNetworkClient client;

    AsyncClient(ConsumerNetworkClient client, LogContext logContext) {
        this.client = client;
        this.log = logContext.logger(getClass());
    }

    public RequestFuture<T2> sendAsyncRequest(Node node, T1 requestData) {
        AbstractRequest.Builder<Req> requestBuilder = prepareRequest(node, requestData);

        return client.send(node, requestBuilder).compose(new RequestFutureAdapter<ClientResponse, T2>() {
            @Override
            @SuppressWarnings("unchecked")
            public void onSuccess(ClientResponse value, RequestFuture<T2> future) {
                Resp resp;
                try {
                    resp = (Resp) value.responseBody();
                } catch (ClassCastException cce) {
                    log.error("Could not cast response body", cce);
                    future.raise(cce);
                    return;
                }
                log.trace("Received {} {} from broker {}", resp.getClass().getSimpleName(), resp, node);
                try {
                    future.complete(handleResponse(node, requestData, resp));
                } catch (RuntimeException e) {
                    if (!future.isDone()) {
                        future.raise(e);
                    }
                }
            }

        });
    }

    protected Logger logger() {
        return log;
    }

    protected abstract AbstractRequest.Builder<Req> prepareRequest(Node node, T1 requestData);

    protected abstract T2 handleResponse(Node node, T1 requestData, Resp response);
}
