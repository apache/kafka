/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.scram;

import org.junit.Test;

import javax.xml.bind.DatatypeConverter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.kafka.common.security.scram.ScramMessages.ClientFinalMessage;
import org.apache.kafka.common.security.scram.ScramMessages.ClientFirstMessage;
import org.apache.kafka.common.security.scram.ScramMessages.ServerFinalMessage;
import org.apache.kafka.common.security.scram.ScramMessages.ServerFirstMessage;

public class ScramFormatterTest {

    /**
     * Tests that the formatter implementation produces the same values for the
     * example included in <a href="https://tools.ietf.org/html/rfc5802#section-5">RFC 5802</a>
     */
    @Test
    public void rfc5802Example() throws Exception {
        ScramFormatter formatter = new ScramFormatter("SHA-1", "HmacSHA1");

        String password = "pencil";
        String c1 = "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL";
        String s1 = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
        String c2 = "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=";
        String s2 = "v=rmF9pqV8S7suAoZWja4dJRkFsKQ=";
        ClientFirstMessage clientFirst = new ClientFirstMessage(formatter.toBytes(c1));
        ServerFirstMessage serverFirst = new ServerFirstMessage(formatter.toBytes(s1));
        ClientFinalMessage clientFinal = new ClientFinalMessage(formatter.toBytes(c2));
        ServerFinalMessage serverFinal = new ServerFinalMessage(formatter.toBytes(s2));

        String username = clientFirst.saslname();
        assertEquals("user", username);
        String clientNonce = clientFirst.nonce();
        assertEquals("fyko+d2lbbFgONRv9qkxdawL", clientNonce);
        String serverNonce = serverFirst.nonce().substring(clientNonce.length());
        assertEquals("3rfcNHYJY1ZVvWVs7j", serverNonce);
        byte[] salt = serverFirst.salt();
        assertArrayEquals(DatatypeConverter.parseBase64Binary("QSXCR+Q6sek8bf92"), salt);
        int iterations = serverFirst.iterations();
        assertEquals(4096, iterations);
        byte[] channelBinding = clientFinal.channelBinding();
        assertArrayEquals(DatatypeConverter.parseBase64Binary("biws"), channelBinding);
        byte[] serverSignature = serverFinal.serverSignature();
        assertArrayEquals(DatatypeConverter.parseBase64Binary("rmF9pqV8S7suAoZWja4dJRkFsKQ="), serverSignature);

        byte[] saltedPassword = formatter.saltedPassword(password, salt, iterations);
        byte[] serverKey = formatter.serverKey(saltedPassword);
        byte[] computedProof = formatter.clientProof(saltedPassword, clientFirst, serverFirst, clientFinal);
        assertArrayEquals(clientFinal.proof(), computedProof);
        byte[] computedSignature = formatter.serverSignature(serverKey, clientFirst, serverFirst, clientFinal);
        assertArrayEquals(serverFinal.serverSignature(), computedSignature);
    }

    /**
     * Tests encoding of username
     */
    @Test
    public void saslName() throws Exception {
        String[] usernames = {"user1", "123", "1,2", "user=A", "user==B", "user,1", "user 1", ",", "=", ",=", "=="};
        ScramFormatter formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_256);
        for (String username : usernames) {
            String saslname = formatter.saslname(username);
            assertEquals(-1, saslname.indexOf(','));
            assertEquals(-1, saslname.replace("=2C", "").replace("=3D", "").indexOf('='));
            assertEquals(username, formatter.username(saslname));
        }
    }
}
