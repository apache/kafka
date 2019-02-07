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
package org.apache.kafka.connect.util.clients;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * The HTTP client which should be used in Connect tests, in order to allow restricted headers to be sent.
 *
 * {@link sun.net.www.protocol.http.HttpURLConnection} has a static block,
 * which reads value of <code>sun.net.http.allowRestrictedHeaders</code>.
 * If any test uses other HTTP client(which depends on <code>HttpURLConnection</code>)
 * that will force classloader to load <code>HttpURLConnection</code>
 * and trigger a static block, which reads <code>sun.net.http.allowRestrictedHeaders</code>.
 * All subsequent writes of this property won't take any effect.
 *
 */
public class HttpClient {

    private final String base;

    public HttpClient(String base) {
        // To be able to set the Origin, we need to toggle this flag
        System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
        this.base = base;
    }

    private Builder buildRequest(String relativePath, MediaType mediaTypes, Map<String, String> headers) {
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(base);
        if (relativePath != null) {
            target = target.path(relativePath);
        }
        Builder request = target.request();
        if (mediaTypes != null) {
            request.accept(mediaTypes);
        }
        if (headers != null) {
            headers.forEach((headerName, headerValue) -> request.header(headerName, headerValue));
        }
        return request;
    }

    public Response executeGet(String path, MediaType mediaTypes, Map<String, String> headers) {
        Builder request = buildRequest(path, mediaTypes, headers);
        return request.get();
    }

    public Response executeGet(String path, Map<String, String> headers) {
        Builder request = buildRequest(path, null, headers);
        return request.get();
    }

    public Response executePut(String path, MediaType mediaTypes, Map<String, String> headers, String body) {
        Builder request = buildRequest(path, mediaTypes, headers);
        return request.put(Entity.entity(body, MediaType.APPLICATION_JSON));
    }

    public Response executePut(Map<String, String> headers, String body) {
        Builder request = buildRequest(null, null, headers);
        return request.put(Entity.entity(body, MediaType.APPLICATION_JSON));
    }

    public Response executePost(String path, MediaType mediaTypes, Map<String, String> headers, String body) {
        Builder request = buildRequest(path, mediaTypes, headers);
        return request.post(Entity.entity(body, MediaType.APPLICATION_JSON));
    }

    public Response executeDelete(String path, MediaType mediaTypes, Map<String, String> headers) {
        Builder request = buildRequest(path, mediaTypes, headers);
        return request.delete();
    }
    public Response executeOptions(String path, MediaType mediaTypes, Map<String, String> headers) {
        Builder request = buildRequest(path, mediaTypes, headers);
        return request.options();
    }

    public Response executeOptions(String path, Map<String, String> headers) {
        Builder request = buildRequest(path, null, headers);
        return request.options();
    }
}
