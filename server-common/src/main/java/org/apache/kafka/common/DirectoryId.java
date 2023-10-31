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
package org.apache.kafka.common;

public class DirectoryId {

    /**
     * A UUID that is used to identify new or unknown dir assignments.
     */
    public static final Uuid UNASSIGNED = new Uuid(0L, 0L);

    /**
     * A UUID that is used to represent unspecified offline dirs.
     */
    public static final Uuid LOST = new Uuid(0L, 1L);

    /**
     * A UUID that is used to represent and unspecified log directory,
     * that is expected to have been previously selected to host an
     * associated replica. This contrasts with {@code UNASSIGNED_DIR},
     * which is associated with (typically new) replicas that may not
     * yet have been placed in any log directory.
     */
    public static final Uuid MIGRATING = new Uuid(0L, 2L);

    /**
     * Static factory to generate a directory ID.
     *
     * This will not generate a reserved UUID (first 100), or one whose string representation
     * starts with a dash ("-")
     */
    public static Uuid random() {
        while (true) {
            // Uuid.randomUuid does not generate Uuids whose string representation starts with a
            // dash.
            Uuid uuid = Uuid.randomUuid();
            if (uuid.getMostSignificantBits() != 0 ||
                    uuid.getLeastSignificantBits() >= 100) {
                return uuid;
            }
        }
    }
}
