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

/**
 * SerializedJwt provides a modicum of structure and validation around a JWT's serialized form by
 * splitting and making the three sections (header, payload, and signature) available to the user.
 */

public class SerializedJwt {

    private final String token;

    private final String header;

    private final String payload;

    private final String signature;

    public SerializedJwt(String token) {
        if (token == null)
            token = "";
        else
            token = token.trim();

        if (token.isEmpty())
            throw new ValidateException("Empty JWT provided; expected three sections (header, payload, and signature)");

        String[] splits = token.split("\\.");

        if (splits.length != 3)
            throw new ValidateException(String.format("Malformed JWT provided (%s); expected three sections (header, payload, and signature), but %s sections provided",
                token, splits.length));

        this.token = token.trim();
        this.header = validateSection(splits[0], "header");
        this.payload = validateSection(splits[1], "payload");
        this.signature = validateSection(splits[2], "signature");
    }

    /**
     * Returns the entire base 64-encoded JWT.
     *
     * @return JWT
     */

    public String getToken() {
        return token;
    }

    /**
     * Returns the first section--the JWT header--in its base 64-encoded form.
     *
     * @return Header section of the JWT
     */

    public String getHeader() {
        return header;
    }

    /**
     * Returns the second section--the JWT payload--in its base 64-encoded form.
     *
     * @return Payload section of the JWT
     */

    public String getPayload() {
        return payload;
    }

    /**
     * Returns the third section--the JWT signature--in its base 64-encoded form.
     *
     * @return Signature section of the JWT
     */

    public String getSignature() {
        return signature;
    }

    private String validateSection(String section, String sectionName) throws ValidateException {
        section = section.trim();

        if (section.isEmpty())
            throw new ValidateException(String.format(
                "Malformed JWT provided; expected at least three sections (header, payload, and signature), but %s section missing",
                sectionName));

        return section;
    }

}
