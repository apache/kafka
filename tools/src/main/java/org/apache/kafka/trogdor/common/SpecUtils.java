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

package org.apache.kafka.trogdor.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;

public class SpecUtils {
    /**
     * @param argument: a JSON string or a path starting with '@'
     * @return String: the JSON SPEC string
     */
    public static String readSpec(String argument) throws IOException {
        if (argument.startsWith("@")) {
            BufferedReader br = null;
            try {
                String specPath = argument.substring(1);
                br = new BufferedReader(
                        new InputStreamReader(new FileInputStream(specPath), StandardCharsets.UTF_8));
                String line;
                StringBuffer spec = new StringBuffer();
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("#"))
                        continue;
                    spec.append(line);
                }
                String specString = spec.toString();
                if (specString.isEmpty()) {
                    throw new IOException("Empty SPEC in the file " + spec);
                }
                return specString;
            } finally {
                if (br != null)
                    br.close();
            }
        } else {
            return argument;
        }
    }
}
