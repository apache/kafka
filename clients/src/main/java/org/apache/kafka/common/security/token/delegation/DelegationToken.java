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
package org.apache.kafka.common.security.token.delegation;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

/**
 * A class representing a delegation token.
 *
 */
@InterfaceStability.Evolving
public class DelegationToken {
    private TokenInformation tokenInformation;
    private byte[] hmac;

    public DelegationToken(TokenInformation tokenInformation, byte[] hmac) {
        this.tokenInformation = tokenInformation;
        this.hmac = hmac;
    }

    public TokenInformation tokenInfo() {
        return tokenInformation;
    }

    public byte[] hmac() {
        return hmac;
    }

    public String hmacAsBase64String() {
        return Base64.getEncoder().encodeToString(hmac);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DelegationToken token = (DelegationToken) o;

        return Objects.equals(tokenInformation, token.tokenInformation) && MessageDigest.isEqual(hmac, token.hmac);
    }

    @Override
    public int hashCode() {
        int result = tokenInformation != null ? tokenInformation.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(hmac);
        return result;
    }

    @Override
    public String toString() {
        return "DelegationToken{" +
            "tokenInformation=" + tokenInformation +
            ", hmac=[*******]" +
            '}';
    }
}
