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

package org.apache.kafka.trogdor.fault;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.kafka.common.utils.Utils;


/**
 * The specification for a fault.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS,
              include = JsonTypeInfo.As.PROPERTY,
              property = "class")
public interface FaultSpec {
    class Util {
        private static final String SPEC_STRING = "Spec";

        public static Fault createFault(String faultId, FaultSpec faultSpec) throws ClassNotFoundException {
            String faultSpecClassName = faultSpec.getClass().getName();
            if (!faultSpecClassName.endsWith(SPEC_STRING)) {
                throw new RuntimeException("FaultSpec class name must end with " + SPEC_STRING);
            }
            String faultClassName = faultSpecClassName.substring(0,
                    faultSpecClassName.length() - SPEC_STRING.length());
            return Utils.newParameterizedInstance(faultClassName,
                String.class, faultId,
                FaultSpec.class, faultSpec);
        }
    }

    /**
     * Get the start time of this fault in ms.
     */
    @JsonProperty
    long startMs();

    /**
     * Get the duration of this fault in ms.
     */
    @JsonProperty
    long durationMs();
}
