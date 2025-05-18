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
package org.apache.kafka.common.security.oauthbearer.internals.secured.assertion;

import org.apache.kafka.common.utils.Time;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A "dynamic" {@link AssertionJwtTemplate} is that which will dynamically add the following values
 * at runtime:
 *
 * <ul>
 *     <li>{@code alg} (Algorithm) header</li>
 *     <li>{@code typ} (Type) header</li>
 *     <li>{@code iat} (Issued at) timestamp claim (in seconds)</li>
 *     <li>{@code exp} (Expiration) timestamp claim (in seconds)</li>
 *     <li>{@code nbf} (Not before) timestamp claim (in seconds)</li>
 *     <li>(Optionally) {@code jti} (JWT ID) claim</li>
 * </ul>
 */
public class DynamicAssertionJwtTemplate implements AssertionJwtTemplate {

    private final Time time;
    private final String algorithm;
    private final int expSeconds;
    private final int nbfSeconds;
    private final boolean includeJti;

    public DynamicAssertionJwtTemplate(Time time,
                                       String algorithm,
                                       int expSeconds,
                                       int nbfSeconds,
                                       boolean includeJti) {
        this.time = time;
        this.algorithm = algorithm;
        this.expSeconds = expSeconds;
        this.nbfSeconds = nbfSeconds;
        this.includeJti = includeJti;
    }

    @Override
    public Map<String, Object> header() {
        Map<String, Object> values = new HashMap<>();
        values.put("alg", algorithm);
        values.put("typ", "JWT");
        return Collections.unmodifiableMap(values);
    }

    @Override
    public Map<String, Object> payload() {
        long currentTimeSecs = time.milliseconds() / 1000L;

        Map<String, Object> values = new HashMap<>();
        values.put("iat", currentTimeSecs);
        values.put("exp", currentTimeSecs + expSeconds);
        values.put("nbf", currentTimeSecs - nbfSeconds);

        if (includeJti)
            values.put("jti", UUID.randomUUID().toString());

        return Collections.unmodifiableMap(values);
    }
}
