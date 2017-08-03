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
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;

public class ProtoUtils {
    public static void walk(Schema schema, SchemaVisitor visitor) {
        if (schema == null || visitor == null) {
            throw new IllegalArgumentException("Both schema and visitor must be provided");
        }
        handleNode(schema, visitor);
    }

    private static void handleNode(Type node, SchemaVisitor visitor) {
        if (node instanceof Schema) {
            Schema schema = (Schema) node;
            visitor.visit(schema);
            for (Field f : schema.fields()) {
                handleNode(f.type, visitor);
            }
        } else if (node instanceof ArrayOf) {
            ArrayOf array = (ArrayOf) node;
            visitor.visit(array);
            handleNode(array.type(), visitor);
        } else {
            visitor.visit(node);
        }
    }
}