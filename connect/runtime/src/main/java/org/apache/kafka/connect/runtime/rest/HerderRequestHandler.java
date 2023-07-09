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
package org.apache.kafka.connect.runtime.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.connect.runtime.distributed.RebalanceNeededException;
import org.apache.kafka.connect.runtime.distributed.RequestTargetException;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class HerderRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(HerderRequestHandler.class);

    private final RestClient restClient;

    private long requestTimeoutMs;

    public HerderRequestHandler(RestClient restClient, long requestTimeoutMs) {
        this.restClient = restClient;
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public void requestTimeoutMs(long requestTimeoutMs) {
        if (requestTimeoutMs < 1) {
            throw new IllegalArgumentException("REST request timeout must be positive");
        }
        this.requestTimeoutMs = requestTimeoutMs;
    }

    /**
     * Wait for a {@link FutureCallback} to complete and return the result if successful.
     * @param cb the future callback to wait for
     * @return the future callback's result if successful
     * @param <T> the future's result type
     * @throws Throwable if the future callback isn't successful
     */
    public <T> T completeRequest(FutureCallback<T> cb) throws Throwable {
        try {
            return cb.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw e.getCause();
        } catch (TimeoutException e) {
            // This timeout is for the operation itself. None of the timeout error codes are relevant, so internal server
            // error is the best option
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "Request timed out");
        } catch (InterruptedException e) {
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "Request interrupted");
        }
    }

    /**
     * Wait for a {@link FutureCallback} to complete. If it succeeds, return the parsed response. If it fails, try to forward the
     * request to the indicated target.
     */
    public <T, U> T completeOrForwardRequest(FutureCallback<T> cb,
                                             String path,
                                             String method,
                                             HttpHeaders headers,
                                             Map<String, String> queryParameters,
                                             Object body,
                                             TypeReference<U> resultType,
                                             Translator<T, U> translator,
                                             Boolean forward) throws Throwable {
        try {
            return completeRequest(cb);
        } catch (RequestTargetException e) {
            if (forward == null || forward) {
                // the only time we allow recursive forwarding is when no forward flag has
                // been set, which should only be seen by the first worker to handle a user request.
                // this gives two total hops to resolve the request before giving up.
                boolean recursiveForward = forward == null;
                String forwardedUrl = e.forwardUrl();
                if (forwardedUrl == null) {
                    // the target didn't know of the leader at this moment.
                    throw new ConnectRestException(Response.Status.CONFLICT.getStatusCode(),
                            "Cannot complete request momentarily due to no known leader URL, "
                                    + "likely because a rebalance was underway.");
                }
                UriBuilder uriBuilder = UriBuilder.fromUri(forwardedUrl)
                        .path(path)
                        .queryParam("forward", recursiveForward);
                if (queryParameters != null) {
                    queryParameters.forEach(uriBuilder::queryParam);
                }
                String forwardUrl = uriBuilder.build().toString();
                log.debug("Forwarding request {} {} {}", forwardUrl, method, body);
                return translator.translate(restClient.httpRequest(forwardUrl, method, headers, body, resultType));
            } else {
                log.error("Request '{} {}' failed because it couldn't find the target Connect worker within two hops (between workers).",
                        method, path);
                // we should find the right target for the query within two hops, so if
                // we don't, it probably means that a rebalance has taken place.
                throw new ConnectRestException(Response.Status.CONFLICT.getStatusCode(),
                        "Cannot complete request because of a conflicting operation (e.g. worker rebalance)");
            }
        } catch (RebalanceNeededException e) {
            throw new ConnectRestException(Response.Status.CONFLICT.getStatusCode(),
                    "Cannot complete request momentarily due to stale configuration (typically caused by a concurrent config change)");
        }
    }

    public <T, U> T completeOrForwardRequest(FutureCallback<T> cb, String path, String method, HttpHeaders headers, Object body,
                                             TypeReference<U> resultType, Translator<T, U> translator, Boolean forward) throws Throwable {
        return completeOrForwardRequest(cb, path, method, headers, null, body, resultType, translator, forward);
    }

    public <T> T completeOrForwardRequest(FutureCallback<T> cb, String path, String method, HttpHeaders headers,
                                          Object body, Boolean forward) throws Throwable {
        return completeOrForwardRequest(cb, path, method, headers, body, null, new IdentityTranslator<>(), forward);
    }

    public interface Translator<T, U> {
        T translate(RestClient.HttpResponse<U> response);
    }

    public static class IdentityTranslator<T> implements Translator<T, T> {
        @Override
        public T translate(RestClient.HttpResponse<T> response) {
            return response.body();
        }
    }
}
