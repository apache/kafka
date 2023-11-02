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
package org.apache.kafka.common.security.scram.internals;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFinalMessage;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFirstMessage;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFirstMessage;

/**
 * Scram message salt and hash functions defined in <a href="https://tools.ietf.org/html/rfc5802">RFC 5802</a>.
 */
public class ScramFormatter {

    private static final Pattern EQUAL = Pattern.compile("=", Pattern.LITERAL);
    private static final Pattern COMMA = Pattern.compile(",", Pattern.LITERAL);
    private static final Pattern EQUAL_TWO_C = Pattern.compile("=2C", Pattern.LITERAL);
    private static final Pattern EQUAL_THREE_D = Pattern.compile("=3D", Pattern.LITERAL);

    private final MessageDigest messageDigest;
    private final Mac mac;
    private final SecureRandom random;

    public ScramFormatter(ScramMechanism mechanism) throws NoSuchAlgorithmException {
        this.messageDigest = MessageDigest.getInstance(mechanism.hashAlgorithm());
        this.mac = Mac.getInstance(mechanism.macAlgorithm());
        this.random = new SecureRandom();
    }

    public byte[] hmac(byte[] key, byte[] bytes) throws InvalidKeyException {
        mac.init(new SecretKeySpec(key, mac.getAlgorithm()));
        return mac.doFinal(bytes);
    }

    public byte[] hash(byte[] str) {
        return messageDigest.digest(str);
    }

    public static byte[] xor(byte[] first, byte[] second) {
        if (first.length != second.length)
            throw new IllegalArgumentException("Argument arrays must be of the same length");
        byte[] result = new byte[first.length];
        for (int i = 0; i < result.length; i++)
            result[i] = (byte) (first[i] ^ second[i]);
        return result;
    }

    public byte[] hi(byte[] str, byte[] salt, int iterations) throws InvalidKeyException {
        mac.init(new SecretKeySpec(str, mac.getAlgorithm()));
        mac.update(salt);
        byte[] u1 = mac.doFinal(new byte[]{0, 0, 0, 1});
        byte[] prev = u1;
        byte[] result = u1;
        for (int i = 2; i <= iterations; i++) {
            byte[] ui = hmac(str, prev);
            result = xor(result, ui);
            prev = ui;
        }
        return result;
    }

    public static byte[] normalize(String str) {
        return toBytes(str);
    }

    public byte[] saltedPassword(String password, byte[] salt, int iterations) throws InvalidKeyException {
        return hi(normalize(password), salt, iterations);
    }

    public byte[] clientKey(byte[] saltedPassword) throws InvalidKeyException {
        return hmac(saltedPassword, toBytes("Client Key"));
    }

    public byte[] storedKey(byte[] clientKey) {
        return hash(clientKey);
    }

    public static String saslName(String username) {
        String replace1 = EQUAL.matcher(username).replaceAll(Matcher.quoteReplacement("=3D"));
        return COMMA.matcher(replace1).replaceAll(Matcher.quoteReplacement("=2C"));
    }

    public static String username(String saslName) {
        String username = EQUAL_TWO_C.matcher(saslName).replaceAll(Matcher.quoteReplacement(","));
        if (EQUAL_THREE_D.matcher(username).replaceAll(Matcher.quoteReplacement("")).indexOf('=') >= 0) {
            throw new IllegalArgumentException("Invalid username: " + saslName);
        }
        return EQUAL_THREE_D.matcher(username).replaceAll(Matcher.quoteReplacement("="));
    }

    public static String authMessage(String clientFirstMessageBare, String serverFirstMessage, String clientFinalMessageWithoutProof) {
        return clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
    }

    public byte[] clientSignature(byte[] storedKey, ClientFirstMessage clientFirstMessage, ServerFirstMessage serverFirstMessage, ClientFinalMessage clientFinalMessage) throws InvalidKeyException {
        byte[] authMessage = authMessage(clientFirstMessage, serverFirstMessage, clientFinalMessage);
        return hmac(storedKey, authMessage);
    }

    public byte[] clientProof(byte[] saltedPassword, ClientFirstMessage clientFirstMessage, ServerFirstMessage serverFirstMessage, ClientFinalMessage clientFinalMessage) throws InvalidKeyException {
        byte[] clientKey = clientKey(saltedPassword);
        byte[] storedKey = hash(clientKey);
        byte[] clientSignature = hmac(storedKey, authMessage(clientFirstMessage, serverFirstMessage, clientFinalMessage));
        return xor(clientKey, clientSignature);
    }

    private byte[] authMessage(ClientFirstMessage clientFirstMessage, ServerFirstMessage serverFirstMessage, ClientFinalMessage clientFinalMessage) {
        return toBytes(authMessage(clientFirstMessage.clientFirstMessageBare(),
                serverFirstMessage.toMessage(),
                clientFinalMessage.clientFinalMessageWithoutProof()));
    }

    public byte[] storedKey(byte[] clientSignature, byte[] clientProof) {
        return hash(xor(clientSignature, clientProof));
    }

    public byte[] serverKey(byte[] saltedPassword) throws InvalidKeyException {
        return hmac(saltedPassword, toBytes("Server Key"));
    }

    public byte[] serverSignature(byte[] serverKey, ClientFirstMessage clientFirstMessage, ServerFirstMessage serverFirstMessage, ClientFinalMessage clientFinalMessage) throws InvalidKeyException {
        byte[] authMessage = authMessage(clientFirstMessage, serverFirstMessage, clientFinalMessage);
        return hmac(serverKey, authMessage);
    }

    public String secureRandomString() {
        return secureRandomString(random);
    }

    public static String secureRandomString(SecureRandom random) {
        return new BigInteger(130, random).toString(Character.MAX_RADIX);
    }

    public byte[] secureRandomBytes() {
        return secureRandomBytes(random);
    }

    public static byte[] secureRandomBytes(SecureRandom random) {
        return toBytes(secureRandomString(random));
    }

    public static byte[] toBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public ScramCredential generateCredential(String password, int iterations) {
        try {
            byte[] salt = secureRandomBytes();
            byte[] saltedPassword = saltedPassword(password, salt, iterations);
            return generateCredential(salt, saltedPassword, iterations);
        } catch (InvalidKeyException e) {
            throw new KafkaException("Could not create credential", e);
        }
    }

    public ScramCredential generateCredential(byte[] salt, byte[] saltedPassword, int iterations) {
        try {
            byte[] clientKey = clientKey(saltedPassword);
            byte[] storedKey = storedKey(clientKey);
            byte[] serverKey = serverKey(saltedPassword);
            return new ScramCredential(salt, storedKey, serverKey, iterations);
        } catch (InvalidKeyException e) {
            throw new KafkaException("Could not create credential", e);
        }
    }
}