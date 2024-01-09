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

package org.apache.kafka.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class CoreUtils {
    /**
     * This method gets comma-separated values which contain key-value pairs and returns a map of
     * key-value pairs. The format of allCSVal is key1:val1, key2:val2 ....
     * Also supports strings with multiple ":" such as IPv6 addresses, taking the last occurrence
     * of the ":" in the pair as the split, e.g., a:b:c:val1, d:e:f:val2 => a:b:c -> val1, d:e:f -> val2
     */
    public static Map<String, String> parseCsvMap(String str) {
        if ("".equals(str)) {
            return new HashMap<>();
        }
        return Arrays.stream(str.split("\\s*,\\s*"))
                .collect(Collectors.toMap(
                        s -> s.substring(0, s.lastIndexOf(":")).trim(),
                        s -> s.substring(s.lastIndexOf(":") + 1).trim(),
                        (oldValue, newValue) -> newValue // select the latest value if there is a duplicate
                ));
    }
}
