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

import java.util.Map;

/**
 * Function to update a schemaless Map based on Field Paths.
 */
@FunctionalInterface
public interface MapValueUpdater {
    // Default function to pass field from original to updated value without modification
    MapValueUpdater PASS_THROUGH_FIELD = (originalParent, updatedParent, fieldPath, fieldName) ->
        updatedParent.put(fieldName, originalParent.get(fieldName));
    // Default function to filter out field on updated value
    MapValueUpdater FILTER_OUT_FIELD = (originalParent, updatedParent, fieldPath, fieldName) -> {
        // filter out
    };

    /**
     * @param originalParent original data object to use as baseline for update
     * @param updatedParent  data object being updated by function
     * @param fieldPath      if match happened, null if applies to other fields
     * @param fieldName      field name
     */
    void apply(
        Map<String, Object> originalParent,
        Map<String, Object> updatedParent,
        SingleFieldPath fieldPath,
        String fieldName
    );
}
