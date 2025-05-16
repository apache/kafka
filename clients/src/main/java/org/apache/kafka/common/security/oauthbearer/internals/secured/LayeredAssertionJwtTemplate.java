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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LayeredAssertionJwtTemplate implements AssertionJwtTemplate {

    private final List<AssertionJwtTemplate> templates;

    public LayeredAssertionJwtTemplate(AssertionJwtTemplate... templates) {
        this.templates = Arrays.asList(templates);
    }

    public LayeredAssertionJwtTemplate(List<AssertionJwtTemplate> templates) {
        this.templates = Collections.unmodifiableList(templates);
    }

    @Override
    public Map<String, Object> header() {
        Map<String, Object> header = new HashMap<>();

        for (AssertionJwtTemplate template : templates)
            header.putAll(template.header());

        return Collections.unmodifiableMap(header);
    }

    @Override
    public Map<String, Object> payload() {
        Map<String, Object> payload = new HashMap<>();

        for (AssertionJwtTemplate template : templates)
            payload.putAll(template.payload());

        return Collections.unmodifiableMap(payload);
    }

    @Override
    public void close() {
        for (AssertionJwtTemplate template : templates) {
            Utils.closeQuietly(template, "JWT assertion template");
        }
    }
}