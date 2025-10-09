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

import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FileAssertionCreatorTest extends OAuthBearerTest {

    @Test
    public void testBasicUsage() throws Exception {
        String expected = createJwt("jdoe");
        File tmpFile = tempFile(expected);

        try (AssertionCreator assertionCreator = new FileAssertionCreator(tmpFile)) {
            String assertion = assertionCreator.create(null);
            assertEquals(expected, assertion);
        }
    }

    @Test
    public void testJwtWithWhitespace() throws Exception {
        String expected = createJwt("jdoe");
        File tmpFile = tempFile("    " + expected + "\n\n\n");

        try (AssertionCreator assertionCreator = new FileAssertionCreator(tmpFile)) {
            String assertion = assertionCreator.create(null);
            assertEquals(expected, assertion);
        }
    }
}
