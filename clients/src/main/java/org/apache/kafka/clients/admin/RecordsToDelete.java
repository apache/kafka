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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

/**
 * Describe records to delete in a call to {@link AdminClient#deleteRecords(Map)}
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class RecordsToDelete {

    private long offset;

    private RecordsToDelete(long offset) {
        this.offset = offset;
    }

    /**
     * Delete all the records before the given {@code offset}
     *
     * @param offset    the offset before which all records will be deleted
     */
    public static RecordsToDelete beforeOffset(long offset) {
        return new RecordsToDelete(offset);
    }

    /**
     * The offset before which all records will be deleted
     */
    public long beforeOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RecordsToDelete that = (RecordsToDelete) o;

        return this.offset == that.offset;
    }

    @Override
    public int hashCode() {
        return (int) offset;
    }

    @Override
    public String toString() {
        return "(beforeOffset = " + offset + ")";
    }
}
