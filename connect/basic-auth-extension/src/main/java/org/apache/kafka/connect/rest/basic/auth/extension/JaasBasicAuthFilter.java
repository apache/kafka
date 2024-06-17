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

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import javax.annotation.Priority;
import javax.security.auth.login.Configuration;
import javax.ws.rs.HttpMethod;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

@Priority(Priorities.AUTHENTICATION)
public class JaasBasicAuthFilter implements ContainerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(JaasBasicAuthFilter.class);
    private static final Set<RequestMatcher> INTERNAL_REQUEST_MATCHERS = new HashSet<>(Arrays.asList(
            new RequestMatcher(HttpMethod.POST, "/?connectors/([^/]+)/tasks/?"),
            new RequestMatcher(HttpMethod.PUT, "/?connectors/[^/]+/fence/?")
    ));
    private static final String CONNECT_LOGIN_MODULE = "KafkaConnect";

    static final String AUTHORIZATION = "Authorization";

    // Package-private for testing
    final Configuration configuration;

    private static class RequestMatcher implements Predicate<ContainerRequestContext> {
        private final String method;
        private final Pattern path;

        public RequestMatcher(String method, String path) {
            this.method = method;
            this.path = Pattern.compile(path);
        }

        @Override
        public boolean test(ContainerRequestContext requestContext) {
            return requestContext.getMethod().equals(method)
                    && path.matcher(requestContext.getUriInfo().getPath()).matches();
        }
    }

    public JaasBasicAuthFilter(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
        if (isInternalRequest(requestContext)) {
            log.trace("Skipping authentication for internal request");
            return;
        }

        try {
            log.debug("Authenticating request");
            BasicAuthCredentials credentials = new BasicAuthCredentials(requestContext.getHeaderString(AUTHORIZATION));
            LoginContext loginContext = new LoginContext(
                CONNECT_LOGIN_MODULE,
                null,
                new BasicAuthCallBackHandler(credentials),
                configuration);
            loginContext.login();
            setSecurityContextForRequest(requestContext, credentials);
        } catch (LoginException | ConfigException e) {
            // Log at debug here in order to avoid polluting log files whenever someone mistypes their credentials
            log.debug("Request failed authentication", e);
            requestContext.abortWith(
                Response.status(Response.Status.UNAUTHORIZED)
                    .entity("User cannot access the resource.")
                    .build());
        }
    }

    private boolean isInternalRequest(ContainerRequestContext requestContext) {
        return INTERNAL_REQUEST_MATCHERS.stream().anyMatch(m -> m.test(requestContext));
    }

    public static class BasicAuthCallBackHandler implements CallbackHandler {

        private final String username;
        private final String password;

        public BasicAuthCallBackHandler(BasicAuthCredentials credentials) {
            username = credentials.username();
            password = credentials.password();
        }

        @Override
        public void handle(Callback[] callbacks) {
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

    private void setSecurityContextForRequest(ContainerRequestContext requestContext, BasicAuthCredentials credential) {
        requestContext.setSecurityContext(new SecurityContext() {
            @Override
            public Principal getUserPrincipal() {
                return credential::username;
            }

            @Override
            public boolean isUserInRole(String role) {
                return false;
            }

            @Override
            public boolean isSecure() {
                return  "https".equalsIgnoreCase(requestContext.getUriInfo().getRequestUri().getScheme());
            }

            @Override
            public String getAuthenticationScheme() {
                return BASIC_AUTH;
            }
        });
    }

    // Visible for testing
    static class BasicAuthCredentials {
        private String username;
        private String password;

        public BasicAuthCredentials(String authorizationHeader) {

            if (authorizationHeader == null) {
                log.trace("No credentials were provided with the request");
                return;
            }

            int spaceIndex = authorizationHeader.indexOf(' ');
            if (spaceIndex <= 0) {
                log.trace("Request credentials were malformed; no space present in value for authorization header");
                return;
            }

            String method = authorizationHeader.substring(0, spaceIndex);
            if (!SecurityContext.BASIC_AUTH.equalsIgnoreCase(method)) {
                log.trace("Request credentials used {} authentication, but only {} supported; ignoring", method, SecurityContext.BASIC_AUTH);
                return;
            }

            authorizationHeader = authorizationHeader.substring(spaceIndex + 1);
            authorizationHeader = new String(Base64.getDecoder().decode(authorizationHeader),
                    StandardCharsets.UTF_8);
            int i = authorizationHeader.indexOf(':');
            if (i <= 0) {
                log.trace("Request credentials were malformed; no colon present between username and password");
                return;
            }

            this.username = authorizationHeader.substring(0, i);
            this.password = authorizationHeader.substring(i + 1);
        }

        public String username() {
            return username;
        }

        public String password() {
            return password;
        }
    }
}
