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

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.Request;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import jakarta.ws.rs.core.HttpHeaders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class InternalRequestSignatureTest {

    private static final byte[] REQUEST_BODY =
            "[{\"config\":\"value\"},{\"config\":\"other_value\"}]".getBytes();
    private static final String SIGNATURE_ALGORITHM = "HmacSHA256";
    private static final SecretKey KEY = new SecretKeySpec(
        new byte[] {
            109, 116, -111, 49, -94, 25, -103, 44, -99, -118, 53, -69, 87, -124, 5, 48,
            89, -105, -2, 58, -92, 87, 67, 49, -125, -79, -39, -126, -51, -53, -85, 57
        }, "HmacSHA256"
    );
    private static final byte[] SIGNATURE = new byte[] {
        42, -3, 127, 57, 43, 49, -51, -43, 72, -62, -10, 120, 123, 125, 26, -65,
        36, 72, 86, -71, -32, 13, -8, 115, 85, 73, -65, -112, 6, 68, 41, -50
    };
    private static final String ENCODED_SIGNATURE = Base64.getEncoder().encodeToString(SIGNATURE);
    private final Crypto crypto = Crypto.SYSTEM;

    @Test
    public void fromHeadersShouldReturnNullOnNullHeaders() {
        assertNull(InternalRequestSignature.fromHeaders(crypto, REQUEST_BODY, null));
    }

    @Test
    public void fromHeadersShouldReturnNullIfSignatureHeaderMissing() {
        assertNull(InternalRequestSignature.fromHeaders(crypto, REQUEST_BODY, internalRequestHeaders(null, SIGNATURE_ALGORITHM)));
    }

    @Test
    public void fromHeadersShouldReturnNullIfSignatureAlgorithmHeaderMissing() {
        assertNull(InternalRequestSignature.fromHeaders(crypto, REQUEST_BODY, internalRequestHeaders(ENCODED_SIGNATURE, null)));
    }

    @Test
    public void fromHeadersShouldThrowExceptionOnInvalidSignatureAlgorithm() {
        assertThrows(BadRequestException.class, () -> InternalRequestSignature.fromHeaders(crypto, REQUEST_BODY,
            internalRequestHeaders(ENCODED_SIGNATURE, "doesn'texist")));
    }

    @Test
    public void fromHeadersShouldThrowExceptionOnInvalidBase64Signature() {
        assertThrows(BadRequestException.class, () -> InternalRequestSignature.fromHeaders(crypto, REQUEST_BODY,
            internalRequestHeaders("not valid base 64", SIGNATURE_ALGORITHM)));
    }

    @Test
    public void fromHeadersShouldReturnNonNullResultOnValidSignatureAndSignatureAlgorithm() {
        InternalRequestSignature signature =
                InternalRequestSignature.fromHeaders(crypto, REQUEST_BODY, internalRequestHeaders(ENCODED_SIGNATURE, SIGNATURE_ALGORITHM));
        assertNotNull(signature);
        assertNotNull(signature.keyAlgorithm());
    }

    @Test
    public void addToRequestShouldThrowExceptionOnInvalidSignatureAlgorithm() throws NoSuchAlgorithmException {
        Request request = mock(Request.class);
        Crypto crypto = mock(Crypto.class);
        when(crypto.mac(anyString())).thenThrow(new NoSuchAlgorithmException("doesn'texist"));
        assertThrows(ConnectException.class, () -> InternalRequestSignature.addToRequest(crypto, KEY, REQUEST_BODY, "doesn'texist", request));
    }

    @Test
    public void addToRequestShouldAddHeadersOnValidSignatureAlgorithm() {
        HttpClient httpClient = new HttpClient();
        Request request = httpClient.newRequest(URI.create("http://localhost"));

        InternalRequestSignature.addToRequest(crypto, KEY, REQUEST_BODY, SIGNATURE_ALGORITHM, request);

        assertEquals(ENCODED_SIGNATURE,
            request.getHeaders().get(InternalRequestSignature.SIGNATURE_HEADER),
            "Request should have valid base 64-encoded signature added as header");
        assertEquals(SIGNATURE_ALGORITHM,
            request.getHeaders().get(InternalRequestSignature.SIGNATURE_ALGORITHM_HEADER),
            "Request should have provided signature algorithm added as header");
    }

    @Test
    public void testSignatureValidation() throws Exception {
        Mac mac = Mac.getInstance(SIGNATURE_ALGORITHM);

        InternalRequestSignature signature = new InternalRequestSignature(REQUEST_BODY, mac, SIGNATURE);
        assertTrue(signature.isValid(KEY));

        signature = InternalRequestSignature.fromHeaders(crypto, REQUEST_BODY, internalRequestHeaders(ENCODED_SIGNATURE, SIGNATURE_ALGORITHM));
        assertTrue(signature.isValid(KEY));

        signature = new InternalRequestSignature("[{\"different_config\":\"different_value\"}]".getBytes(), mac, SIGNATURE);
        assertFalse(signature.isValid(KEY));

        signature = new InternalRequestSignature(REQUEST_BODY, mac, "bad signature".getBytes());
        assertFalse(signature.isValid(KEY));
    }

    private static HttpHeaders internalRequestHeaders(String signature, String signatureAlgorithm) {
        HttpHeaders result = mock(HttpHeaders.class);
        when(result.getHeaderString(eq(InternalRequestSignature.SIGNATURE_HEADER)))
            .thenReturn(signature);
        when(result.getHeaderString(eq(InternalRequestSignature.SIGNATURE_ALGORITHM_HEADER)))
            .thenReturn(signatureAlgorithm);
        return result;
    }
}
