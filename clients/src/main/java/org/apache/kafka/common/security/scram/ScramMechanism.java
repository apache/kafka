/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.scram;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum ScramMechanism {

    SCRAM_SHA_256("SHA-256", "HmacSHA256", 4096),
    SCRAM_SHA_512("SHA-512", "HmacSHA512", 4096);

    private final String mechanismName;
    private final String hashAlgorithm;
    private final String macAlgorithm;
    private final int minIterations;

    private static final Map<String, ScramMechanism> MECHANISMS_MAP;

    static {
        Map<String, ScramMechanism> map = new HashMap<>();
        for (ScramMechanism mech : values())
            map.put(mech.mechanismName, mech);
        MECHANISMS_MAP = Collections.unmodifiableMap(map);
    }

    ScramMechanism(String hashAlgorithm, String macAlgorithm, int minIterations) {
        this.mechanismName = "SCRAM-" + hashAlgorithm;
        this.hashAlgorithm = hashAlgorithm;
        this.macAlgorithm = macAlgorithm;
        this.minIterations = minIterations;
    }

    public final String mechanismName() {
        return mechanismName;
    }

    public String hashAlgorithm() {
        return hashAlgorithm;
    }

    public String macAlgorithm() {
        return macAlgorithm;
    }

    public int minIterations() {
        return minIterations;
    }

    public static ScramMechanism forMechanismName(String mechanismName) {
        return MECHANISMS_MAP.get(mechanismName);
    }

    public static Collection<String> mechanismNames() {
        return MECHANISMS_MAP.keySet();
    }

    public static boolean isScram(String mechanismName) {
        return MECHANISMS_MAP.containsKey(mechanismName);
    }
}
