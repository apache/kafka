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
package org.apache.kafka.common.network;

import java.util.Objects;

public class CipherInformation {
    private final String cipher;
    private final String protocol;

    public CipherInformation(String cipher, String protocol) {
        this.cipher = cipher == null || cipher.isEmpty()  ? "unknown" : cipher;
        this.protocol = protocol == null || protocol.isEmpty()  ? "unknown" : protocol;
    }

    public String cipher() {
        return cipher;
    }

    public String protocol() {
        return protocol;
    }

    @Override
    public String toString() {
        return "CipherInformation(cipher=" + cipher +
            ", protocol=" + protocol + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(cipher, protocol);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof CipherInformation)) {
            return false;
        }
        CipherInformation other = (CipherInformation) o;
        return other.cipher.equals(cipher) &&
            other.protocol.equals(protocol);
    }
}
