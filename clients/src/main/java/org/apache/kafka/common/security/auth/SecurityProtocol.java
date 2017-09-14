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
package org.apache.kafka.common.security.auth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public enum SecurityProtocol {
    /** Un-authenticated, non-encrypted channel */
    PLAINTEXT(0, "PLAINTEXT"),
    /** SSL channel */
    SSL(1, "SSL"),
    /** SASL authenticated, non-encrypted channel */
    SASL_PLAINTEXT(2, "SASL_PLAINTEXT"),
    /** SASL authenticated, SSL channel */
    SASL_SSL(3, "SASL_SSL");

    private static final Map<Short, SecurityProtocol> CODE_TO_SECURITY_PROTOCOL;
    private static final List<String> NAMES;

    static {
        SecurityProtocol[] protocols = SecurityProtocol.values();
        List<String> names = new ArrayList<>(protocols.length);
        Map<Short, SecurityProtocol> codeToSecurityProtocol = new HashMap<>(protocols.length);
        for (SecurityProtocol proto : protocols) {
            codeToSecurityProtocol.put(proto.id, proto);
            names.add(proto.name);
        }
        CODE_TO_SECURITY_PROTOCOL = Collections.unmodifiableMap(codeToSecurityProtocol);
        NAMES = Collections.unmodifiableList(names);
    }

    /** The permanent and immutable id of a security protocol -- this can't change, and must match kafka.cluster.SecurityProtocol  */
    public final short id;

    /** Name of the security protocol. This may be used by client configuration. */
    public final String name;

    SecurityProtocol(int id, String name) {
        this.id = (short) id;
        this.name = name;
    }

    public static List<String> names() {
        return NAMES;
    }

    public static SecurityProtocol forId(short id) {
        return CODE_TO_SECURITY_PROTOCOL.get(id);
    }

    /** Case insensitive lookup by protocol name */
    public static SecurityProtocol forName(String name) {
        return SecurityProtocol.valueOf(name.toUpperCase(Locale.ROOT));
    }

}
