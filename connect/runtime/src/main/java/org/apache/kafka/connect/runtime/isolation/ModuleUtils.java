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
package org.apache.kafka.connect.runtime.isolation;

public class ModuleUtils {
    private static final String BLACKLIST = "^(?:"
            + "java\\."
            + "|javax\\."
            + "|org\\.w3c\\.dom"
            + "|org\\.apache\\.kafka\\.common"
            + "|org\\.apache\\.kafka\\.connect"
            + "|org\\.apache\\.log4j"
            + ")\\..*$";

    private static final String WHITELIST = "^org\\.apache\\.kafka\\.connect\\.(?:"
            + "transforms\\.(?!Transformation$).*"
            + "|json\\..*"
            + "|file\\..*"
            + "|converters\\..*"
            + "|storage\\.StringConverter"
            + ")$";

    public static boolean validate(String name) {
        //boolean result = name.matches(BLACKLIST) && !name.matches(WHITELIST);
        if (name.equals("org.apache.kafka.connect.transforms.Transformation")) {
            boolean yeap = true;
        }
        boolean result = name.matches(BLACKLIST);
        if (result) {
            result = !name.matches(WHITELIST);
        }
        return !result;
    }
}
