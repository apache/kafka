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
package org.apache.kafka.connect.transforms.field;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Function to update Struct schemas based on Field Paths.
 *
 * @see FieldPath
 * @see org.apache.kafka.connect.data.Schema
 */
@FunctionalInterface
public interface StructSchemaUpdater {

    /**
     * Apply schema update function.
     *
     * @param schemaBuilder builder to be updated. Required, not nullable.
     * @param field nullable when updating fields that do not exist. e.g. paths not found.
     * @param fieldPath nullable when updating a field that is not related to a path.
     */
    void apply(SchemaBuilder schemaBuilder, Field field, SingleFieldPath fieldPath);
}
