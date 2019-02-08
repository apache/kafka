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

import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.sasl.SaslException;

/**
 * SCRAM request/response message creation and parsing based on
 * <a href="https://tools.ietf.org/html/rfc5802">RFC 5802</a>
 *
 */
public class ScramMessages {

    static abstract class AbstractScramMessage {

        static final String ALPHA = "[A-Za-z]+";
        static final String VALUE_SAFE = "[\\x01-\\x7F&&[^=,]]+";
        static final String VALUE = "[\\x01-\\x7F&&[^,]]+";
        static final String PRINTABLE = "[\\x21-\\x7E&&[^,]]+";
        static final String SASLNAME = "(?:[\\x01-\\x7F&&[^=,]]|=2C|=3D)+";
        static final String BASE64_CHAR = "[a-zA-Z0-9/+]";
        static final String BASE64 = String.format("(?:%s{4})*(?:%s{3}=|%s{2}==)?", BASE64_CHAR, BASE64_CHAR, BASE64_CHAR);
        static final String RESERVED = String.format("(m=%s,)?", VALUE);
        static final String EXTENSIONS = String.format("(,%s=%s)*", ALPHA, VALUE);

        abstract String toMessage();

        public byte[] toBytes() {
            return toMessage().getBytes(StandardCharsets.UTF_8);
        }

        protected String toMessage(byte[] messageBytes) {
            return new String(messageBytes, StandardCharsets.UTF_8);
        }
    }

    /**
     * Format:
     *     gs2-header [reserved-mext ","] username "," nonce ["," extensions]
     * Limitations:
     *     Only gs2-header "n" is supported.
     *     Extensions are ignored.
     *
     */
    public static class ClientFirstMessage extends AbstractScramMessage {
        private static final Pattern PATTERN = Pattern.compile(String.format(
                "n,(a=(?<authzid>%s))?,%sn=(?<saslname>%s),r=(?<nonce>%s)(?<extensions>%s)",
                SASLNAME,
                RESERVED,
                SASLNAME,
                PRINTABLE,
                EXTENSIONS));


        private final String saslName;
        private final String nonce;
        private final String authorizationId;
        private final ScramExtensions extensions;
        public ClientFirstMessage(byte[] messageBytes) throws SaslException {
            String message = toMessage(messageBytes);
            Matcher matcher = PATTERN.matcher(message);
            if (!matcher.matches())
                throw new SaslException("Invalid SCRAM client first message format: " + message);
            String authzid = matcher.group("authzid");
            this.authorizationId = authzid != null ? authzid : "";
            this.saslName = matcher.group("saslname");
            this.nonce = matcher.group("nonce");
            String extString = matcher.group("extensions");

            this.extensions = extString.startsWith(",") ? new ScramExtensions(extString.substring(1)) : new ScramExtensions();
        }
        public ClientFirstMessage(String saslName, String nonce, Map<String, String> extensions) {
            this.saslName = saslName;
            this.nonce = nonce;
            this.extensions = new ScramExtensions(extensions);
            this.authorizationId = ""; // Optional authzid not specified in gs2-header
        }
        public String saslName() {
            return saslName;
        }
        public String nonce() {
            return nonce;
        }
        public String authorizationId() {
            return authorizationId;
        }
        public String gs2Header() {
            return "n," + authorizationId + ",";
        }
        public ScramExtensions extensions() {
            return extensions;
        }

        public String clientFirstMessageBare() {
            String extensionStr = Utils.mkString(extensions.map(), "", "", "=", ",");

            if (extensionStr.isEmpty())
                return String.format("n=%s,r=%s", saslName, nonce);
            else
                return String.format("n=%s,r=%s,%s", saslName, nonce, extensionStr);
        }
        String toMessage() {
            return gs2Header() + clientFirstMessageBare();
        }
    }

    /**
     * Format:
     *     [reserved-mext ","] nonce "," salt "," iteration-count ["," extensions]
     * Limitations:
     *     Extensions are ignored.
     *
     */
    public static class ServerFirstMessage extends AbstractScramMessage {
        private static final Pattern PATTERN = Pattern.compile(String.format(
                "%sr=(?<nonce>%s),s=(?<salt>%s),i=(?<iterations>[0-9]+)%s",
                RESERVED,
                PRINTABLE,
                BASE64,
                EXTENSIONS));

        private final String nonce;
        private final byte[] salt;
        private final int iterations;
        public ServerFirstMessage(byte[] messageBytes) throws SaslException {
            String message = toMessage(messageBytes);
            Matcher matcher = PATTERN.matcher(message);
            if (!matcher.matches())
                throw new SaslException("Invalid SCRAM server first message format: " + message);
            try {
                this.iterations = Integer.parseInt(matcher.group("iterations"));
                if (this.iterations <= 0)
                    throw new SaslException("Invalid SCRAM server first message format: invalid iterations " + iterations);
            } catch (NumberFormatException e) {
                throw new SaslException("Invalid SCRAM server first message format: invalid iterations");
            }
            this.nonce = matcher.group("nonce");
            String salt = matcher.group("salt");
            this.salt = Base64.getDecoder().decode(salt);
        }
        public ServerFirstMessage(String clientNonce, String serverNonce, byte[] salt, int iterations) {
            this.nonce = clientNonce + serverNonce;
            this.salt = salt;
            this.iterations = iterations;
        }
        public String nonce() {
            return nonce;
        }
        public byte[] salt() {
            return salt;
        }
        public int iterations() {
            return iterations;
        }
        String toMessage() {
            return String.format("r=%s,s=%s,i=%d", nonce, Base64.getEncoder().encodeToString(salt), iterations);
        }
    }
    /**
     * Format:
     *     channel-binding "," nonce ["," extensions]"," proof
     * Limitations:
     *     Extensions are ignored.
     *
     */
    public static class ClientFinalMessage extends AbstractScramMessage {
        private static final Pattern PATTERN = Pattern.compile(String.format(
                "c=(?<channel>%s),r=(?<nonce>%s)%s,p=(?<proof>%s)",
                BASE64,
                PRINTABLE,
                EXTENSIONS,
                BASE64));

        private final byte[] channelBinding;
        private final String nonce;
        private byte[] proof;
        public ClientFinalMessage(byte[] messageBytes) throws SaslException {
            String message = toMessage(messageBytes);
            Matcher matcher = PATTERN.matcher(message);
            if (!matcher.matches())
                throw new SaslException("Invalid SCRAM client final message format: " + message);

            this.channelBinding = Base64.getDecoder().decode(matcher.group("channel"));
            this.nonce = matcher.group("nonce");
            this.proof = Base64.getDecoder().decode(matcher.group("proof"));
        }
        public ClientFinalMessage(byte[] channelBinding, String nonce) {
            this.channelBinding = channelBinding;
            this.nonce = nonce;
        }
        public byte[] channelBinding() {
            return channelBinding;
        }
        public String nonce() {
            return nonce;
        }
        public byte[] proof() {
            return proof;
        }
        public void proof(byte[] proof) {
            this.proof = proof;
        }
        public String clientFinalMessageWithoutProof() {
            return String.format("c=%s,r=%s",
                    Base64.getEncoder().encodeToString(channelBinding),
                    nonce);
        }
        String toMessage() {
            return String.format("%s,p=%s",
                    clientFinalMessageWithoutProof(),
                    Base64.getEncoder().encodeToString(proof));
        }
    }
    /**
     * Format:
     *     ("e=" server-error-value | "v=" base64_server_signature) ["," extensions]
     * Limitations:
     *     Extensions are ignored.
     *
     */
    public static class ServerFinalMessage extends AbstractScramMessage {
        private static final Pattern PATTERN = Pattern.compile(String.format(
                "(?:e=(?<error>%s))|(?:v=(?<signature>%s))%s",
                VALUE_SAFE,
                BASE64,
                EXTENSIONS));

        private final String error;
        private final byte[] serverSignature;
        public ServerFinalMessage(byte[] messageBytes) throws SaslException {
            String message = toMessage(messageBytes);
            Matcher matcher = PATTERN.matcher(message);
            if (!matcher.matches())
                throw new SaslException("Invalid SCRAM server final message format: " + message);
            String error = null;
            try {
                error = matcher.group("error");
            } catch (IllegalArgumentException e) {
                // ignore
            }
            if (error == null) {
                this.serverSignature = Base64.getDecoder().decode(matcher.group("signature"));
                this.error = null;
            } else {
                this.serverSignature = null;
                this.error = error;
            }
        }
        public ServerFinalMessage(String error, byte[] serverSignature) {
            this.error = error;
            this.serverSignature = serverSignature;
        }
        public String error() {
            return error;
        }
        public byte[] serverSignature() {
            return serverSignature;
        }
        String toMessage() {
            if (error != null)
                return "e=" + error;
            else
                return "v=" + Base64.getEncoder().encodeToString(serverSignature);
        }
    }
}
