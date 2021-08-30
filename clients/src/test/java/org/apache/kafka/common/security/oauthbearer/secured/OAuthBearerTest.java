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

package org.apache.kafka.common.security.oauthbearer.secured;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.function.Executable;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class OAuthBearerTest {

    protected ObjectMapper mapper;

    @BeforeAll
    public void setup() {
        mapper = new ObjectMapper();
    }

    protected void assertThrowsWithMessage(Class<? extends Exception> clazz,
        Executable validatorExecutable,
        String substring) {
        assertThrows(clazz, validatorExecutable);

        try {
            validatorExecutable.execute();
        } catch (Throwable e) {
            assertTrue(e.getMessage().contains(substring),
                String.format("Expected exception message (\"%s\") to contain substring (\"%s\")",
                    e.getMessage(), substring));
        }
    }

    protected String createBase64JsonJwtSection(Consumer<ObjectNode> c) {
        String json = createJsonJwtSection(c);

        try {
            return Utils.utf8(Base64.getEncoder().encode(Utils.utf8(json)));
        } catch (Throwable t) {
            fail(t);

            // Shouldn't get to here...
            return null;
        }
    }

    protected String createJsonJwtSection(Consumer<ObjectNode> c) {
        ObjectNode node = mapper.createObjectNode();
        c.accept(node);

        try {
            return mapper.writeValueAsString(node);
        } catch (Throwable t) {
            fail(t);

            // Shouldn't get to here...
            return null;
        }
    }

    protected Retryable<String> createRetryable(Exception[] attempts) {
        AtomicInteger counter = new AtomicInteger(0);

        return () -> {
            int currAttempt = counter.getAndIncrement();
            Exception e = attempts[currAttempt];

            if (e == null) {
                return "success!";
            } else {
                if (e instanceof IOException)
                    throw (IOException) e;
                else if (e instanceof RuntimeException)
                    throw (RuntimeException) e;
                else
                    throw new RuntimeException(e);
            }
        };
    }

    protected HttpURLConnection createHttpURLConnection(String response,
        int statusCode)
        throws IOException {
        HttpURLConnection mockedCon = mock(HttpURLConnection.class);
        when(mockedCon.getResponseCode()).thenReturn(statusCode);
        when(mockedCon.getOutputStream()).thenReturn(new ByteArrayOutputStream());
        when(mockedCon.getInputStream()).thenReturn(new ByteArrayInputStream(Utils.utf8(response)));
        return mockedCon;
    }

}