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
package org.apache.kafka.connect.json;

/**
 * Represents the valid {@link org.apache.kafka.connect.data.Decimal} serialization formats
 * in a {@link JsonConverter}.
 */
public enum DecimalFormat {

    /**
     * Serializes the JSON Decimal as a base-64 string. For example, serializing the value
     * `10.2345` with the BASE64 setting will result in `"D3J5"`.
     */
    BASE64,

    /**
     * Serializes the JSON Decimal as a JSON number. For example, serializing the value
     * `10.2345` with the NUMERIC setting will result in `10.2345`.
     */
    NUMERIC
}
