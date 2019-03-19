package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AsyncClient<T1, Req extends AbstractRequest, Resp extends AbstractResponse, T2> {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ConsumerNetworkClient client;

    AsyncClient(ConsumerNetworkClient client) {
        this.client = client;
    }

    public RequestFuture<T2> sendAsyncRequest(Node node, T1 requestData) {
        AbstractRequest.Builder<Req> requestBuilder = prepareRequest(node, requestData);

        return client.send(node, requestBuilder).compose(new RequestFutureAdapter<ClientResponse, T2>() {
            @Override
            @SuppressWarnings("unchecked")
            public void onSuccess(ClientResponse value, RequestFuture<T2> future1) {
                Resp resp;
                try {
                    resp = (Resp) value.responseBody();
                } catch (ClassCastException cce) {
                    log.error("Could not cast response body", cce);
                    future1.raise(cce);
                    return;
                }
                log.trace("Received {} {} from broker {}", resp.getClass().getSimpleName(), resp, node);
                try {
                    future1.complete(handleResponse(node, requestData, resp));
                } catch (RuntimeException t) {
                    future1.raise(t);
                }
            }

            @Override
            public void onFailure(RuntimeException e, RequestFuture<T2> future1) {
                future1.raise(e);
            }
        });
    }

    protected abstract AbstractRequest.Builder<Req> prepareRequest(Node node, T1 requestData);

    protected abstract T2 handleResponse(Node node, T1 requestData, Resp response);
}
