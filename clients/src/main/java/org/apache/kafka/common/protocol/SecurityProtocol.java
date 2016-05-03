/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public enum SecurityProtocol {
    /** Un-authenticated, non-encrypted channel */
    PLAINTEXT(0, "PLAINTEXT", false),
    /** SSL channel */
    SSL(1, "SSL", false),
    /** SASL authenticated, non-encrypted channel */
    SASL_PLAINTEXT(2, "SASL_PLAINTEXT", false),
    /** SASL authenticated, SSL channel */
    SASL_SSL(3, "SASL_SSL", false),
    /** Currently identical to PLAINTEXT and used for testing only. We may implement extra instrumentation when testing channel code. */
    TRACE(Short.MAX_VALUE, "TRACE", true);

    private static final Map<Short, SecurityProtocol> CODE_TO_SECURITY_PROTOCOL;
    private static final List<String> NAMES;
    private static final Set<SecurityProtocol> NON_TESTING_VALUES;

    static {
        SecurityProtocol[] protocols = SecurityProtocol.values();
        List<String> names = new ArrayList<>(protocols.length);
        Map<Short, SecurityProtocol> codeToSecurityProtocol = new HashMap<>(protocols.length);
        Set<SecurityProtocol> nonTestingValues = EnumSet.noneOf(SecurityProtocol.class);
        for (SecurityProtocol proto : protocols) {
            codeToSecurityProtocol.put(proto.id, proto);
            names.add(proto.name);
            if (!proto.isTesting)
                nonTestingValues.add(proto);
        }
        CODE_TO_SECURITY_PROTOCOL = Collections.unmodifiableMap(codeToSecurityProtocol);
        NAMES = Collections.unmodifiableList(names);
        NON_TESTING_VALUES = Collections.unmodifiableSet(nonTestingValues);
    }

    /** The permanent and immutable id of a security protocol -- this can't change, and must match kafka.cluster.SecurityProtocol  */
    public final short id;

    /** Name of the security protocol. This may be used by client configuration. */
    public final String name;

    /* Whether this security protocol is for testing/debugging */
    private final boolean isTesting;

    private SecurityProtocol(int id, String name, boolean isTesting) {
        this.id = (short) id;
        this.name = name;
        this.isTesting = isTesting;
    }

    public static String getName(int id) {
        return CODE_TO_SECURITY_PROTOCOL.get((short) id).name;
    }

    public static List<String> getNames() {
        return NAMES;
    }

    public static SecurityProtocol forId(Short id) {
        return CODE_TO_SECURITY_PROTOCOL.get(id);
    }

    /** Case insensitive lookup by protocol name */
    public static SecurityProtocol forName(String name) {
        return SecurityProtocol.valueOf(name.toUpperCase(Locale.ROOT));
    }

    /**
     * Returns the set of non-testing SecurityProtocol instances, that is, SecurityProtocol instances that are suitable
     * for production usage.
     */
    public static Set<SecurityProtocol> nonTestingValues() {
        return NON_TESTING_VALUES;
    }

}
