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

package org.apache.kafka.connect.rest.basic.auth.extension;

import java.util.regex.Pattern;
import javax.ws.rs.HttpMethod;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;

public class JaasBasicAuthFilter implements ContainerRequestFilter {
    private static final String CONNECT_LOGIN_MODULE = "KafkaConnect";
    static final String AUTHORIZATION = "Authorization";
    private static final Pattern TASK_REQUEST_PATTERN = Pattern.compile("/?connectors/([^/]+)/tasks/?");
    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        try {
            if (!(requestContext.getMethod().equals(HttpMethod.POST) && TASK_REQUEST_PATTERN.matcher(requestContext.getUriInfo().getPath()).matches())) {
                LoginContext loginContext =
                    new LoginContext(CONNECT_LOGIN_MODULE, new BasicAuthCallBackHandler(
                        requestContext.getHeaderString(AUTHORIZATION)));
                loginContext.login();
            }
        } catch (LoginException | ConfigException e) {
            requestContext.abortWith(
                Response.status(Response.Status.UNAUTHORIZED)
                    .entity("User cannot access the resource.")
                    .build());
        }
    }


    public static class BasicAuthCallBackHandler implements CallbackHandler {

        private static final String BASIC = "basic";
        private static final char COLON = ':';
        private static final char SPACE = ' ';
        private String username;
        private String password;

        public BasicAuthCallBackHandler(String credentials) {
            if (credentials != null) {
                int space = credentials.indexOf(SPACE);
                if (space > 0) {
                    String method = credentials.substring(0, space);
                    if (BASIC.equalsIgnoreCase(method)) {
                        credentials = credentials.substring(space + 1);
                        credentials = new String(Base64.getDecoder().decode(credentials),
                                                 StandardCharsets.UTF_8);
                        int i = credentials.indexOf(COLON);
                        if (i > 0) {
                            username = credentials.substring(0, i);
                            password = credentials.substring(i + 1);
                        }
                    }
                }
            }
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    ((NameCallback) callback).setName(username);
                } else if (callback instanceof PasswordCallback) {
                    ((PasswordCallback) callback).setPassword(password.toCharArray());
                } else {
                    throw new UnsupportedCallbackException(callback, "Supports only NameCallback "
                                                                     + "and PasswordCallback");
                }
            }
        }
    }
}
