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
package org.apache.kafka.connect.runtime.rest;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.distributed.Crypto;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;

import org.eclipse.jetty.client.Request;

import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

import javax.crypto.Mac;
import javax.crypto.SecretKey;

import jakarta.ws.rs.core.HttpHeaders;

public class InternalRequestSignature {

    public static final String SIGNATURE_HEADER = "X-Connect-Authorization";
    public static final String SIGNATURE_ALGORITHM_HEADER = "X-Connect-Request-Signature-Algorithm";

    private final byte[] requestBody;
    private final Mac mac;
    private final byte[] requestSignature;

    /**
     * Add a signature to a request.
     *
     * @param crypto             the cryptography library used to generate {@link Mac} instances, may not be null
     * @param key                the key to sign the request with; may not be null
     * @param requestBody        the body of the request; may not be null
     * @param signatureAlgorithm the algorithm to use to sign the request; may not be null
     * @param request            the request to add the signature to; may not be null
     */
    public static void addToRequest(Crypto crypto, SecretKey key, byte[] requestBody, String signatureAlgorithm, Request request) {
        Mac mac;
        try {
            mac = crypto.mac(signatureAlgorithm);
        }  catch (NoSuchAlgorithmException e) {
            throw new ConnectException(e);
        }
        byte[] requestSignature = sign(mac, key, requestBody);
        request.headers(field -> {
            field.add(InternalRequestSignature.SIGNATURE_HEADER, Base64.getEncoder().encodeToString(requestSignature));
            field.add(InternalRequestSignature.SIGNATURE_ALGORITHM_HEADER, signatureAlgorithm);
        });
    }

    /**
     * Extract a signature from a request.
     *
     * @param crypto        the cryptography library used to generate {@link Mac} instances, may not be null
     * @param requestBody   the body of the request; may not be null
     * @param headers       the headers for the request; may be null
     * @return the signature extracted from the request, or null if one or more request signature
     * headers was not present
     */
    public static InternalRequestSignature fromHeaders(Crypto crypto, byte[] requestBody, HttpHeaders headers) {
        if (headers == null) {
            return null;
        }

        String signatureAlgorithm = headers.getHeaderString(SIGNATURE_ALGORITHM_HEADER);
        String encodedSignature = headers.getHeaderString(SIGNATURE_HEADER);
        if (signatureAlgorithm == null || encodedSignature == null) {
            return null;
        }

        Mac mac;
        try {
            mac = crypto.mac(signatureAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new BadRequestException(e.getMessage());
        }

        byte[] decodedSignature;
        try {
            decodedSignature = Base64.getDecoder().decode(encodedSignature);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException(e.getMessage());
        }

        return new InternalRequestSignature(
            requestBody,
            mac,
            decodedSignature
        );
    }

    // Public for testing
    public InternalRequestSignature(byte[] requestBody, Mac mac, byte[] requestSignature) {
        this.requestBody = requestBody;
        this.mac = mac;
        this.requestSignature = requestSignature;
    }

    public String keyAlgorithm() {
        return mac.getAlgorithm();
    }

    public boolean isValid(SecretKey key) {
        return MessageDigest.isEqual(sign(mac, key, requestBody), requestSignature);
    }

    private static byte[] sign(Mac mac, SecretKey key, byte[] requestBody) {
        try {
            mac.init(key);
        } catch (InvalidKeyException e) {
            throw new ConnectException(e);
        }
        return mac.doFinal(requestBody);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        InternalRequestSignature that = (InternalRequestSignature) o;
        return MessageDigest.isEqual(requestBody, that.requestBody)
            && mac.getAlgorithm().equals(that.mac.getAlgorithm())
            && mac.getMacLength() == that.mac.getMacLength()
            && mac.getProvider().equals(that.mac.getProvider())
            && MessageDigest.isEqual(requestSignature, that.requestSignature);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(mac);
        result = 31 * result + Arrays.hashCode(requestBody);
        result = 31 * result + Arrays.hashCode(requestSignature);
        return result;
    }
}
