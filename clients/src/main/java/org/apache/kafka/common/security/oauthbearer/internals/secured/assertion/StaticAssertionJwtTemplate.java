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

import java.util.Collections;
import java.util.Map;

/**
 * This {@link AssertionJwtTemplate} uses a static set of headers and claims provided on initialization.
 * The values typically come from configuration, and it is often used in conjunction with other templates
 * such as {@link LayeredAssertionJwtTemplate}.
 */
public class StaticAssertionJwtTemplate implements AssertionJwtTemplate {

    private final Map<String, Object> header;

    private final Map<String, Object> payload;

    public StaticAssertionJwtTemplate() {
        this.header = Map.of();
        this.payload = Map.of();
    }

    public StaticAssertionJwtTemplate(Map<String, Object> header, Map<String, Object> payload) {
        this.header = Collections.unmodifiableMap(header);
        this.payload = Collections.unmodifiableMap(payload);
    }

    @Override
    public Map<String, Object> header() {
        return header;
    }

    @Override
    public Map<String, Object> payload() {
        return payload;
    }
}
