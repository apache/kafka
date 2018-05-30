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

import org.apache.kafka.common.security.scram.internal.ScramMessages.ClientFinalMessage;
import org.apache.kafka.common.security.scram.internal.ScramMessages.ClientFirstMessage;
import org.apache.kafka.common.security.scram.internal.ScramMessages.ServerFinalMessage;
import org.apache.kafka.common.security.scram.internal.ScramMessages.ServerFirstMessage;

import org.junit.Test;

import java.util.Base64;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ScramFormatterTest {

    /**
     * Tests that the formatter implementation produces the same values for the
     * example included in <a href="https://tools.ietf.org/html/rfc5802#section-5">RFC 7677</a>
     */
    @Test
    public void rfc7677Example() throws Exception {
        ScramFormatter formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_256);

        String password = "pencil";
        String c1 = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
        String s1 = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        String c2 = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
        String s2 = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";
        ClientFirstMessage clientFirst = new ClientFirstMessage(formatter.toBytes(c1));
        ServerFirstMessage serverFirst = new ServerFirstMessage(formatter.toBytes(s1));
        ClientFinalMessage clientFinal = new ClientFinalMessage(formatter.toBytes(c2));
        ServerFinalMessage serverFinal = new ServerFinalMessage(formatter.toBytes(s2));

        String username = clientFirst.saslName();
        assertEquals("user", username);
        String clientNonce = clientFirst.nonce();
        assertEquals("rOprNGfwEbeRWgbNEkqO", clientNonce);
        String serverNonce = serverFirst.nonce().substring(clientNonce.length());
        assertEquals("%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0", serverNonce);
        byte[] salt = serverFirst.salt();
        assertArrayEquals(Base64.getDecoder().decode("W22ZaJ0SNY7soEsUEjb6gQ=="), salt);
        int iterations = serverFirst.iterations();
        assertEquals(4096, iterations);
        byte[] channelBinding = clientFinal.channelBinding();
        assertArrayEquals(Base64.getDecoder().decode("biws"), channelBinding);
        byte[] serverSignature = serverFinal.serverSignature();
        assertArrayEquals(Base64.getDecoder().decode("6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="), serverSignature);

        byte[] saltedPassword = formatter.saltedPassword(password, salt, iterations);
        byte[] serverKey = formatter.serverKey(saltedPassword);
        byte[] computedProof = formatter.clientProof(saltedPassword, clientFirst, serverFirst, clientFinal);
        assertArrayEquals(clientFinal.proof(), computedProof);
        byte[] computedSignature = formatter.serverSignature(serverKey, clientFirst, serverFirst, clientFinal);
        assertArrayEquals(serverFinal.serverSignature(), computedSignature);

        // Minimum iterations defined in RFC-7677
        assertEquals(4096, ScramMechanism.SCRAM_SHA_256.minIterations());
    }

    /**
     * Tests encoding of username
     */
    @Test
    public void saslName() throws Exception {
        String[] usernames = {"user1", "123", "1,2", "user=A", "user==B", "user,1", "user 1", ",", "=", ",=", "=="};
        ScramFormatter formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_256);
        for (String username : usernames) {
            String saslName = formatter.saslName(username);
            // There should be no commas in saslName (comma is used as field separator in SASL messages)
            assertEquals(-1, saslName.indexOf(','));
            // There should be no "=" in the saslName apart from those used in encoding (comma is =2C and equals is =3D)
            assertEquals(-1, saslName.replace("=2C", "").replace("=3D", "").indexOf('='));
            assertEquals(username, formatter.username(saslName));
        }
    }
}
