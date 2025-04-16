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
package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.security.oauthbearer.JwtRetriever;
import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.Set;

/**
 * A {@link JwtRetriever} that will communicate with an OAuth/OIDC provider directly via HTTP.
 *
 * @see JwtRetriever
 */
public final class JwtHttpClient {

    private static final Logger log = LoggerFactory.getLogger(JwtHttpClient.class);

    public static final Set<Integer> DEFAULT_SUCCESS_STATUS_CODES;

    public static final Set<Integer> DEFAULT_FATAL_STATUS_CODES;

    static {
        DEFAULT_SUCCESS_STATUS_CODES = Set.of(
            HttpURLConnection.HTTP_OK,
            HttpURLConnection.HTTP_CREATED
        );

        // This does not have to be an exhaustive list. There are other HTTP codes that
        // are defined in different RFCs (e.g. https://datatracker.ietf.org/doc/html/rfc6585)
        // that we won't worry about yet. The worst case if a status code is missing from
        // this set is that the request will be retried.
        DEFAULT_FATAL_STATUS_CODES = Set.of(
            HttpURLConnection.HTTP_BAD_REQUEST,
            HttpURLConnection.HTTP_UNAUTHORIZED,
            HttpURLConnection.HTTP_PAYMENT_REQUIRED,
            HttpURLConnection.HTTP_FORBIDDEN,
            HttpURLConnection.HTTP_NOT_FOUND,
            HttpURLConnection.HTTP_BAD_METHOD,
            HttpURLConnection.HTTP_NOT_ACCEPTABLE,
            HttpURLConnection.HTTP_PROXY_AUTH,
            HttpURLConnection.HTTP_CONFLICT,
            HttpURLConnection.HTTP_GONE,
            HttpURLConnection.HTTP_LENGTH_REQUIRED,
            HttpURLConnection.HTTP_PRECON_FAILED,
            HttpURLConnection.HTTP_ENTITY_TOO_LARGE,
            HttpURLConnection.HTTP_REQ_TOO_LONG,
            HttpURLConnection.HTTP_UNSUPPORTED_TYPE,
            HttpURLConnection.HTTP_NOT_IMPLEMENTED,
            HttpURLConnection.HTTP_VERSION
        );
    }

    private final Time time;
    private final Set<Integer> successStatusCodes;
    private final Set<Integer> fatalStatusCodes;

    public JwtHttpClient(Time time) {
        this(time, DEFAULT_SUCCESS_STATUS_CODES, DEFAULT_FATAL_STATUS_CODES);
    }

    public JwtHttpClient(Time time,
                         Set<Integer> successStatusCodes,
                         Set<Integer> fatalStatusCodes) {
        this.time = time;
        this.successStatusCodes = Collections.unmodifiableSet(successStatusCodes);
        this.fatalStatusCodes = Collections.unmodifiableSet(fatalStatusCodes);
    }

    public String request(HttpClient client,
                          HttpRequest request,
                          HttpResponse.BodyHandler<String> responseHandler,
                          long retryBackoffMs,
                          long retryBackoffMaxMs) throws JwtRetrieverException {
        long endMs = time.milliseconds() + retryBackoffMaxMs;
        int currAttempt = 0;

        while (time.milliseconds() <= endMs) {
            currAttempt++;

            try {
                HttpResponse<String> response = client.send(request, responseHandler);
                int statusCode = response.statusCode();

                if (successStatusCodes.contains(statusCode)) {
                    return response.body();
                } else if (fatalStatusCodes.contains(statusCode)) {
                    // This is a non-transient error, so don't bother retrying the request unnecessarily.
                    throw new JwtRetrieverException(
                        String.format(
                            "The fatal status code %s was encountered on attempt %s to retrieve the JWT from the OAuth token endpoint",
                            statusCode,
                            currAttempt
                        )
                    );
                } else {
                    long waitMs = retryBackoffMs * (long) Math.pow(2, currAttempt - 1);
                    long diff = endMs - time.milliseconds();
                    waitMs = Math.min(waitMs, diff);

                    if (waitMs <= 0)
                        break;

                    log.warn(
                        "The status code {} was encountered on attempt {} to retrieve the JWT from the OAuth token endpoint; sleeping {} ms before attempting again",
                        statusCode,
                        currAttempt,
                        waitMs
                    );

                    time.sleep(waitMs);
                }
            } catch (IOException | InterruptedException e) {
                throw new JwtRetrieverException(e);
            }
        }

        throw new JwtRetrieverException(
            String.format(
                "%s failed attempts were made to retrieve the JWT from the OAuth token endpoint; will not attempt further",
                currAttempt
            )
        );
    }
}
