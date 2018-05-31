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
package org.apache.kafka.common.security.scram.internal;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;

import javax.security.sasl.SaslException;

import org.apache.kafka.common.security.scram.internal.ScramMessages.AbstractScramMessage;
import org.apache.kafka.common.security.scram.internal.ScramMessages.ClientFinalMessage;
import org.apache.kafka.common.security.scram.internal.ScramMessages.ClientFirstMessage;
import org.apache.kafka.common.security.scram.internal.ScramMessages.ServerFinalMessage;
import org.apache.kafka.common.security.scram.internal.ScramMessages.ServerFirstMessage;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ScramMessagesTest {

    private static final String[] VALID_EXTENSIONS = {
        "ext=val1",
        "anotherext=name1=value1 name2=another test value \"\'!$[]()",
        "first=val1,second=name1 = value ,third=123"
    };
    private static final String[] INVALID_EXTENSIONS = {
        "ext1=value",
        "ext",
        "ext=value1,value2",
        "ext=,",
        "ext =value"
    };

    private static final String[] VALID_RESERVED = {
        "m=reserved-value",
        "m=name1=value1 name2=another test value \"\'!$[]()"
    };
    private static final String[] INVALID_RESERVED = {
        "m",
        "m=name,value",
        "m=,"
    };

    private ScramFormatter formatter;

    @Before
    public void setUp() throws Exception {
        formatter  = new ScramFormatter(ScramMechanism.SCRAM_SHA_256);
    }

    @Test
    public void validClientFirstMessage() throws SaslException {
        String nonce = formatter.secureRandomString();
        ClientFirstMessage m = new ClientFirstMessage("someuser", nonce, Collections.<String, String>emptyMap());
        checkClientFirstMessage(m, "someuser", nonce, "");

        // Default format used by Kafka client: only user and nonce are specified
        String str = String.format("n,,n=testuser,r=%s", nonce);
        m = createScramMessage(ClientFirstMessage.class, str);
        checkClientFirstMessage(m, "testuser", nonce, "");
        m = new ClientFirstMessage(m.toBytes());
        checkClientFirstMessage(m, "testuser", nonce, "");

        // Username containing comma, encoded as =2C
        str = String.format("n,,n=test=2Cuser,r=%s", nonce);
        m = createScramMessage(ClientFirstMessage.class, str);
        checkClientFirstMessage(m, "test=2Cuser", nonce, "");
        assertEquals("test,user", formatter.username(m.saslName()));

        // Username containing equals, encoded as =3D
        str = String.format("n,,n=test=3Duser,r=%s", nonce);
        m = createScramMessage(ClientFirstMessage.class, str);
        checkClientFirstMessage(m, "test=3Duser", nonce, "");
        assertEquals("test=user", formatter.username(m.saslName()));

        // Optional authorization id specified
        str = String.format("n,a=testauthzid,n=testuser,r=%s", nonce);
        checkClientFirstMessage(createScramMessage(ClientFirstMessage.class, str), "testuser", nonce, "testauthzid");

        // Optional reserved value specified
        for (String reserved : VALID_RESERVED) {
            str = String.format("n,,%s,n=testuser,r=%s", reserved, nonce);
            checkClientFirstMessage(createScramMessage(ClientFirstMessage.class, str), "testuser", nonce, "");
        }

        // Optional extension specified
        for (String extension : VALID_EXTENSIONS) {
            str = String.format("n,,n=testuser,r=%s,%s", nonce, extension);
            checkClientFirstMessage(createScramMessage(ClientFirstMessage.class, str), "testuser", nonce, "");
        }

        //optional tokenauth specified as extensions
        str = String.format("n,,n=testuser,r=%s,%s", nonce, "tokenauth=true");
        m = createScramMessage(ClientFirstMessage.class, str);
        assertTrue("Token authentication not set from extensions", m.extensions().tokenAuthenticated());
    }

    @Test
    public void invalidClientFirstMessage() throws SaslException {
        String nonce = formatter.secureRandomString();
        // Invalid entry in gs2-header
        String invalid = String.format("n,x=something,n=testuser,r=%s", nonce);
        checkInvalidScramMessage(ClientFirstMessage.class, invalid);

        // Invalid reserved entry
        for (String reserved : INVALID_RESERVED) {
            invalid = String.format("n,,%s,n=testuser,r=%s", reserved, nonce);
            checkInvalidScramMessage(ClientFirstMessage.class, invalid);
        }

        // Invalid extension
        for (String extension : INVALID_EXTENSIONS) {
            invalid = String.format("n,,n=testuser,r=%s,%s", nonce, extension);
            checkInvalidScramMessage(ClientFirstMessage.class, invalid);
        }
    }

    @Test
    public void validServerFirstMessage() throws SaslException {
        String clientNonce = formatter.secureRandomString();
        String serverNonce = formatter.secureRandomString();
        String nonce = clientNonce + serverNonce;
        String salt = randomBytesAsString();

        ServerFirstMessage m = new ServerFirstMessage(clientNonce, serverNonce, toBytes(salt), 8192);
        checkServerFirstMessage(m, nonce, salt, 8192);

        // Default format used by Kafka clients, only nonce, salt and iterations are specified
        String str = String.format("r=%s,s=%s,i=4096", nonce, salt);
        m = createScramMessage(ServerFirstMessage.class, str);
        checkServerFirstMessage(m, nonce, salt, 4096);
        m = new ServerFirstMessage(m.toBytes());
        checkServerFirstMessage(m, nonce, salt, 4096);

        // Optional reserved value
        for (String reserved : VALID_RESERVED) {
            str = String.format("%s,r=%s,s=%s,i=4096", reserved, nonce, salt);
            checkServerFirstMessage(createScramMessage(ServerFirstMessage.class, str), nonce, salt, 4096);
        }

        // Optional extension
        for (String extension : VALID_EXTENSIONS) {
            str = String.format("r=%s,s=%s,i=4096,%s", nonce, salt, extension);
            checkServerFirstMessage(createScramMessage(ServerFirstMessage.class, str), nonce, salt, 4096);
        }
    }

    @Test
    public void invalidServerFirstMessage() throws SaslException {
        String nonce = formatter.secureRandomString();
        String salt = randomBytesAsString();

        // Invalid iterations
        String invalid = String.format("r=%s,s=%s,i=0", nonce, salt);
        checkInvalidScramMessage(ServerFirstMessage.class, invalid);

        // Invalid salt
        invalid = String.format("r=%s,s=%s,i=4096", nonce, "=123");
        checkInvalidScramMessage(ServerFirstMessage.class, invalid);

        // Invalid format
        invalid = String.format("r=%s,invalid,s=%s,i=4096", nonce, salt);
        checkInvalidScramMessage(ServerFirstMessage.class, invalid);

        // Invalid reserved entry
        for (String reserved : INVALID_RESERVED) {
            invalid = String.format("%s,r=%s,s=%s,i=4096", reserved, nonce, salt);
            checkInvalidScramMessage(ServerFirstMessage.class, invalid);
        }

        // Invalid extension
        for (String extension : INVALID_EXTENSIONS) {
            invalid = String.format("r=%s,s=%s,i=4096,%s", nonce, salt, extension);
            checkInvalidScramMessage(ServerFirstMessage.class, invalid);
        }
    }

    @Test
    public void validClientFinalMessage() throws SaslException {
        String nonce = formatter.secureRandomString();
        String channelBinding = randomBytesAsString();
        String proof = randomBytesAsString();

        ClientFinalMessage m = new ClientFinalMessage(toBytes(channelBinding), nonce);
        assertNull("Invalid proof", m.proof());
        m.proof(toBytes(proof));
        checkClientFinalMessage(m, channelBinding, nonce, proof);

        // Default format used by Kafka client: channel-binding, nonce and proof are specified
        String str = String.format("c=%s,r=%s,p=%s", channelBinding, nonce, proof);
        m = createScramMessage(ClientFinalMessage.class, str);
        checkClientFinalMessage(m, channelBinding, nonce, proof);
        m = new ClientFinalMessage(m.toBytes());
        checkClientFinalMessage(m, channelBinding, nonce, proof);

        // Optional extension specified
        for (String extension : VALID_EXTENSIONS) {
            str = String.format("c=%s,r=%s,%s,p=%s", channelBinding, nonce, extension, proof);
            checkClientFinalMessage(createScramMessage(ClientFinalMessage.class, str), channelBinding, nonce, proof);
        }
    }

    @Test
    public void invalidClientFinalMessage() throws SaslException {
        String nonce = formatter.secureRandomString();
        String channelBinding = randomBytesAsString();
        String proof = randomBytesAsString();

        // Invalid channel binding
        String invalid = String.format("c=ab,r=%s,p=%s", nonce, proof);
        checkInvalidScramMessage(ClientFirstMessage.class, invalid);

        // Invalid proof
        invalid = String.format("c=%s,r=%s,p=123", channelBinding, nonce);
        checkInvalidScramMessage(ClientFirstMessage.class, invalid);

        // Invalid extensions
        for (String extension : INVALID_EXTENSIONS) {
            invalid = String.format("c=%s,r=%s,%s,p=%s", channelBinding, nonce, extension, proof);
            checkInvalidScramMessage(ClientFinalMessage.class, invalid);
        }
    }

    @Test
    public void validServerFinalMessage() throws SaslException {
        String serverSignature = randomBytesAsString();

        ServerFinalMessage m = new ServerFinalMessage("unknown-user", null);
        checkServerFinalMessage(m, "unknown-user", null);
        m = new ServerFinalMessage(null, toBytes(serverSignature));
        checkServerFinalMessage(m, null, serverSignature);

        // Default format used by Kafka clients for successful final message
        String str = String.format("v=%s", serverSignature);
        m = createScramMessage(ServerFinalMessage.class, str);
        checkServerFinalMessage(m, null, serverSignature);
        m = new ServerFinalMessage(m.toBytes());
        checkServerFinalMessage(m, null, serverSignature);

        // Default format used by Kafka clients for final message with error
        str = "e=other-error";
        m = createScramMessage(ServerFinalMessage.class, str);
        checkServerFinalMessage(m, "other-error", null);
        m = new ServerFinalMessage(m.toBytes());
        checkServerFinalMessage(m, "other-error", null);

        // Optional extension
        for (String extension : VALID_EXTENSIONS) {
            str = String.format("v=%s,%s", serverSignature, extension);
            checkServerFinalMessage(createScramMessage(ServerFinalMessage.class, str), null, serverSignature);
        }
    }

    @Test
    public void invalidServerFinalMessage() throws SaslException {
        String serverSignature = randomBytesAsString();

        // Invalid error
        String invalid = "e=error1,error2";
        checkInvalidScramMessage(ServerFinalMessage.class, invalid);

        // Invalid server signature
        invalid = String.format("v=1=23");
        checkInvalidScramMessage(ServerFinalMessage.class, invalid);

        // Invalid extensions
        for (String extension : INVALID_EXTENSIONS) {
            invalid = String.format("v=%s,%s", serverSignature, extension);
            checkInvalidScramMessage(ServerFinalMessage.class, invalid);

            invalid = String.format("e=unknown-user,%s", extension);
            checkInvalidScramMessage(ServerFinalMessage.class, invalid);
        }
    }

    private String randomBytesAsString() {
        return Base64.getEncoder().encodeToString(formatter.secureRandomBytes());
    }

    private byte[] toBytes(String base64Str) {
        return Base64.getDecoder().decode(base64Str);
    };

    private void checkClientFirstMessage(ClientFirstMessage message, String saslName, String nonce, String authzid) {
        assertEquals(saslName, message.saslName());
        assertEquals(nonce, message.nonce());
        assertEquals(authzid, message.authorizationId());
    }

    private void checkServerFirstMessage(ServerFirstMessage message, String nonce, String salt, int iterations) {
        assertEquals(nonce, message.nonce());
        assertArrayEquals(Base64.getDecoder().decode(salt), message.salt());
        assertEquals(iterations, message.iterations());
    }

    private void checkClientFinalMessage(ClientFinalMessage message, String channelBinding, String nonce, String proof) {
        assertArrayEquals(Base64.getDecoder().decode(channelBinding), message.channelBinding());
        assertEquals(nonce, message.nonce());
        assertArrayEquals(Base64.getDecoder().decode(proof), message.proof());
    }

    private void checkServerFinalMessage(ServerFinalMessage message, String error, String serverSignature) {
        assertEquals(error, message.error());
        if (serverSignature == null)
            assertNull("Unexpected server signature", message.serverSignature());
        else
            assertArrayEquals(Base64.getDecoder().decode(serverSignature), message.serverSignature());
    }

    @SuppressWarnings("unchecked")
    private <T extends AbstractScramMessage> T createScramMessage(Class<T> clazz, String message) throws SaslException {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        if (clazz == ClientFirstMessage.class)
            return (T) new ClientFirstMessage(bytes);
        else if (clazz == ServerFirstMessage.class)
            return (T) new ServerFirstMessage(bytes);
        else if (clazz == ClientFinalMessage.class)
            return (T) new ClientFinalMessage(bytes);
        else if (clazz == ServerFinalMessage.class)
            return (T) new ServerFinalMessage(bytes);
        else
            throw new IllegalArgumentException("Unknown message type: " + clazz);
    }

    private <T extends AbstractScramMessage> void checkInvalidScramMessage(Class<T> clazz, String message) {
        try {
            createScramMessage(clazz, message);
            fail("Exception not throws for invalid message of type " + clazz + " : " + message);
        } catch (SaslException e) {
            // Expected exception
        }
    }
}
