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

package org.apache.kafka.message;

import java.io.BufferedWriter;
import java.io.IOException;

public interface TypeClassGenerator {
    /**
     * The short name of the type class file we are generating. For example,
     * ApiMessageType.java.
     */
    String outputName();

    /**
     * Registers a message spec with the generator.
     *
     * @param spec      The spec to register.
     */
    void registerMessageType(MessageSpec spec);

    /**
     * Generate the type, and then write it out.
     *
     * @param writer    The writer to write out the state to.
     */
    void generateAndWrite(BufferedWriter writer) throws IOException;
}
