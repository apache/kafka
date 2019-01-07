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
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;

public class SpecUtils {
    /**
     * @param argument: a JSON string or a path starting with @

     */
    public static String readSpec(String argument) throws IOException {
        if (argument.startsWith("@")) {
            String specPath = argument.substring(1);
            BufferedReader br = new BufferedReader(new FileReader(specPath));
            String line;
            String spec = "";
            while ((line = br.readLine()) != null)
            {
                if (line.startsWith("#"))
                    continue;
                spec += line;
            }
            if (spec.isEmpty())
            {
                throw new IOException("Empty SPEC in the file " + spec);
            }
            return spec;
        } else {
            return argument;
        }
    }
}
