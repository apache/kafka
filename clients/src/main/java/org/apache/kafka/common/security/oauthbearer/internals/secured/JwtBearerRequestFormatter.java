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


import org.apache.kafka.common.utils.Utils;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class JwtBearerRequestFormatter implements HttpRequestFormatter {

    public static final String GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer";

    private final String scope;
    private final Supplier<String> assertionSupplier;

    public JwtBearerRequestFormatter(String scope, Supplier<String> assertionSupplier) {
        this.scope = scope;
        this.assertionSupplier = assertionSupplier;
    }

    @Override
    public String formatBody() {
        String assertion = assertionSupplier.get();
        StringBuilder requestParameters = new StringBuilder();
        requestParameters.append("grant_type=").append(URLEncoder.encode(GRANT_TYPE, StandardCharsets.UTF_8));
        requestParameters.append("&assertion=").append(URLEncoder.encode(assertion, StandardCharsets.UTF_8));

        if (!Utils.isBlank(scope))
            requestParameters.append("&scope=").append(URLEncoder.encode(scope.trim(), StandardCharsets.UTF_8));

        return requestParameters.toString();
    }

    @Override
    public Map<String, String> formatHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Accept", "application/json");
        headers.put("Cache-Control", "no-cache");
        headers.put("Content-Type", "application/x-www-form-urlencoded");
        return headers;
    }
}
