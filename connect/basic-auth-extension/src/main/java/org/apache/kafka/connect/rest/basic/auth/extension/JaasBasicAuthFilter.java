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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import javax.security.auth.login.Configuration;
import javax.ws.rs.HttpMethod;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log = LoggerFactory.getLogger(JaasBasicAuthFilter.class);
    private static final Pattern TASK_REQUEST_PATTERN = Pattern.compile("/?connectors/([^/]+)/tasks/?");
    private static final String CONNECT_LOGIN_MODULE = "KafkaConnect";

    static final String AUTHORIZATION = "Authorization";

    // Package-private for testing
    final Configuration configuration;

    public JaasBasicAuthFilter(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (isInternalTaskConfigRequest(requestContext)) {
            log.trace("Skipping authentication for internal request");
            return;
        }

        try {
            log.debug("Authenticating request");
            LoginContext loginContext = new LoginContext(
                CONNECT_LOGIN_MODULE,
                null,
                new BasicAuthCallBackHandler(requestContext.getHeaderString(AUTHORIZATION)),
                configuration);
            loginContext.login();
        } catch (LoginException | ConfigException e) {
            // Log at debug here in order to avoid polluting log files whenever someone mistypes their credentials
            log.debug("Request failed authentication", e);
            requestContext.abortWith(
                Response.status(Response.Status.UNAUTHORIZED)
                    .entity("User cannot access the resource.")
                    .build());
        }
    }

    private static boolean isInternalTaskConfigRequest(ContainerRequestContext requestContext) {
        return requestContext.getMethod().equals(HttpMethod.POST)
            && TASK_REQUEST_PATTERN.matcher(requestContext.getUriInfo().getPath()).matches();
    }


    public static class BasicAuthCallBackHandler implements CallbackHandler {

        private static final String BASIC = "basic";
        private static final char COLON = ':';
        private static final char SPACE = ' ';
        private String username;
        private String password;

        public BasicAuthCallBackHandler(String credentials) {
            if (credentials == null) {
                log.trace("No credentials were provided with the request");
                return;
            }

            int space = credentials.indexOf(SPACE);
            if (space <= 0) {
                log.trace("Request credentials were malformed; no space present in value for authorization header");
                return;
            }

            String method = credentials.substring(0, space);
            if (!BASIC.equalsIgnoreCase(method)) {
                log.trace("Request credentials used {} authentication, but only {} supported; ignoring", method, BASIC);
                return;
            }

            credentials = credentials.substring(space + 1);
            credentials = new String(Base64.getDecoder().decode(credentials),
                                     StandardCharsets.UTF_8);
            int i = credentials.indexOf(COLON);
            if (i <= 0) {
                log.trace("Request credentials were malformed; no colon present between username and password");
                return;
            }

            username = credentials.substring(0, i);
            password = credentials.substring(i + 1);
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            List<Callback> unsupportedCallbacks = new ArrayList<>();
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    ((NameCallback) callback).setName(username);
                } else if (callback instanceof PasswordCallback) {
                    ((PasswordCallback) callback).setPassword(password != null
                        ? password.toCharArray()
                        : null
                    );
                } else {
                    unsupportedCallbacks.add(callback);
                }
            }
            if (!unsupportedCallbacks.isEmpty())
                throw new ConnectException(String.format(
                    "Unsupported callbacks %s; request authentication will fail. "
                        + "This indicates the Connect worker was configured with a JAAS "
                        + "LoginModule that is incompatible with the %s, and will need to be "
                        + "corrected and restarted.",
                    unsupportedCallbacks,
                    BasicAuthSecurityRestExtension.class.getSimpleName()
                ));
        }
    }
}
