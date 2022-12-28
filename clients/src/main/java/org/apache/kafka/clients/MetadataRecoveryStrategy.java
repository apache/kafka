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
package org.apache.kafka.clients;

import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

import static java.util.Arrays.asList;

/**
 * Defines the strategies which clients can follow to deal with the situation when none of the known nodes is available.
 */
public enum MetadataRecoveryStrategy {
    NONE("none"),
    REBOOTSTRAP("rebootstrap");

    public final String name;

    MetadataRecoveryStrategy(String name) {
        this.name = name;
    }

    private static final List<MetadataRecoveryStrategy> VALUES = asList(values());

    public static MetadataRecoveryStrategy forName(String n) {
        String name = n.toLowerCase(Locale.ROOT);
        for (MetadataRecoveryStrategy t : values()) {
            if (t.name.equals(name)) {
                return t;
            }
        }
        throw new NoSuchElementException("Invalid metadata recovery strategy " + name);
    }
}
