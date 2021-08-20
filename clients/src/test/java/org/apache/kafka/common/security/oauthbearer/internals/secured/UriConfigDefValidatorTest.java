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

import org.apache.kafka.common.config.ConfigDef.Validator;
import org.junit.jupiter.api.Test;

public class UriConfigDefValidatorTest extends AbstractConfigDefValidatorTest {

    @Override
    public Validator createValidator() {
        return new UriConfigDefValidator();
    }

    @Test
    public void test() {
        ensureValid("http://www.example.com");
    }

    @Test
    public void testWithSuperfluousWhitespace() {
        ensureValid(String.format("  %s  ", "http://www.example.com"));
    }

    @Test
    public void testCaseInsensitivity() {
        ensureValid("HTTPS://WWW.EXAMPLE.COM");
    }

    @Test
    public void testFullPath() {
        ensureValid("https://myidp.example.com/oauth2/default/v1/token");
    }

    @Test
    public void testMissingScheme() {
        assertThrowsWithMessage(() -> ensureValid("www.example.com"), "missing the scheme");
    }

    @Test
    public void testInvalidScheme() {
        assertThrowsWithMessage(() -> ensureValid("ftp://www.example.com"), "invalid scheme");
    }


}
