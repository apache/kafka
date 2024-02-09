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
package org.apache.kafka.server.util;

import java.util.HashMap;
import java.util.Map;

public class Csv {

    /**
     * This method gets comma separated values which contains key,value pairs and returns a map of
     * key value pairs. the format of allCSVal is key1:val1, key2:val2 ....
     * Also supports strings with multiple ":" such as IpV6 addresses, taking the last occurrence
     * of the ":" in the pair as the split, eg a:b:c:val1, d:e:f:val2 => a:b:c -> val1, d:e:f -> val2
     */
    public static Map<String, String> parseCsvMap(String str) {
        Map<String, String> map = new HashMap<>();
        if (str == null || "".equals(str))
            return map;
        String[] keyVals = str.split("\\s*,\\s*");
        for (String s : keyVals) {
            int lio = s.lastIndexOf(":");
            map.put(s.substring(0, lio).trim(), s.substring(lio + 1).trim());
        }
        return map;
    }
}
