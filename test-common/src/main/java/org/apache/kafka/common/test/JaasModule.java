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
package org.apache.kafka.common.test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public record JaasModule(String name, boolean debug, Map<String, String> entries) {

    public static JaasModule plainLoginModule(String username, String password, boolean debug, Map<String, String> validUsers) {
        String name = "org.apache.kafka.common.security.plain.PlainLoginModule";

        Map<String, String> entries = new HashMap<>();
        entries.put("username", username);
        entries.put("password", password);
        validUsers.forEach((user, pass) -> entries.put("user_" + user, pass));

        return new JaasModule(
            name,
            debug,
            entries
        );
    }

    @Override
    public String toString() {
        return String.format("%s required%n  debug=%b%n  %s;%n", name, debug, entries.entrySet().stream()
                .map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
                .collect(Collectors.joining("\n  ")));
    }
}
