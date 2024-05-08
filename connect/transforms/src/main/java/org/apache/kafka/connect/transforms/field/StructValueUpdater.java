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
import org.apache.kafka.connect.data.Struct;

/**
 * Function to update a Struct based on Field Paths.
 *
 * @see org.apache.kafka.connect.data.Struct
 */
@FunctionalInterface
public interface StructValueUpdater {

    // Default function to filter out field on updated value
    StructValueUpdater FILTER_OUT_VALUE = (originalParent, originalField, updatedParent, updatedField, fieldPath) -> {
        // filter out
    };
    // Default function to filter out field on updated value
    StructValueUpdater PASS_THROUGH_VALUE = (originalParent, originalField, updatedParent, nullUpdatedField, nullFieldPath) ->
        updatedParent.put(originalField.name(), originalParent.get(originalField));

    /**
     * @param originalParent parent struct to use as baseline for update
     * @param originalField  original field to use as baseline
     * @param updatedParent  struct to be updated by applying function
     * @param updatedField   field to be changed by function
     * @param fieldPath      if match happened, null if applies to other fields
     */
    void apply(
        Struct originalParent,
        Field originalField,
        Struct updatedParent,
        Field updatedField,
        SingleFieldPath fieldPath
    );
}
