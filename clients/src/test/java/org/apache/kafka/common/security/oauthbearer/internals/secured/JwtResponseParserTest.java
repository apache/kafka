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

import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JwtResponseParserTest extends OAuthBearerTest {

    @Test
    public void testParseJwt() throws IOException {
        String expected = "abc";
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("access_token", expected);

        JwtResponseParser responseParser = new JwtResponseParser();
        String actual = responseParser.parseJwt(mapper.writeValueAsString(node));
        assertEquals(expected, actual);
    }

    @Test
    public void testParseJwtEmptyAccessToken() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("access_token", "");

        JwtResponseParser responseParser = new JwtResponseParser();
        assertThrows(JwtRetrieverException.class, () -> responseParser.parseJwt(mapper.writeValueAsString(node)));
    }

    @Test
    public void testParseJwtMissingAccessToken() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("sub", "jdoe");

        JwtResponseParser responseParser = new JwtResponseParser();
        assertThrows(JwtRetrieverException.class, () -> responseParser.parseJwt(mapper.writeValueAsString(node)));
    }

    @Test
    public void testParseJwtInvalidJson() {
        JwtResponseParser responseParser = new JwtResponseParser();
        assertThrows(JwtRetrieverException.class, () -> responseParser.parseJwt("not valid JSON"));
    }
}
