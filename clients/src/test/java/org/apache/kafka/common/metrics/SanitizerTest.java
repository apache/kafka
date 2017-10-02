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
package org.apache.kafka.common.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;

import org.junit.Test;

public class SanitizerTest {

    @Test
    public void testSanitize() throws UnsupportedEncodingException {
        String principal = "CN=Some characters !@#$%&*()_-+=';:,/~";
        String sanitizedPrincipal = Sanitizer.sanitize(principal);
        assertTrue(sanitizedPrincipal.replace('%', '_').matches("[a-zA-Z0-9\\._\\-]+"));
        assertEquals(principal, Sanitizer.desanitize(sanitizedPrincipal));
    }
}
