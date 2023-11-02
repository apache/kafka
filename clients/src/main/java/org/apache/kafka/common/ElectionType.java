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

package org.apache.kafka.common;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Arrays;
import java.util.Set;

/**
 * Options for {@link org.apache.kafka.clients.admin.Admin#electLeaders(ElectionType, Set, org.apache.kafka.clients.admin.ElectLeadersOptions)}.
 *
 * The API of this class is evolving, see {@link org.apache.kafka.clients.admin.Admin} for details.
 */
@InterfaceStability.Evolving
public enum ElectionType {
    PREFERRED((byte) 0), UNCLEAN((byte) 1);

    public final byte value;

    ElectionType(byte value) {
        this.value = value;
    }

    public static ElectionType valueOf(byte value) {
        if (value == PREFERRED.value) {
            return PREFERRED;
        } else if (value == UNCLEAN.value) {
            return UNCLEAN;
        } else {
            throw new IllegalArgumentException(
                    String.format("Value %s must be one of %s", value, Arrays.asList(ElectionType.values())));
        }
    }
}
