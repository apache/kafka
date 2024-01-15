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
package org.apache.kafka.streams.processor.api;

public final class InternalFixedKeyRecordFactory {

    private InternalFixedKeyRecordFactory() {
    }

    /**
     * Only allowed way to create {@link FixedKeyRecord}s.
     * <p/>
     * DO NOT USE THIS FACTORY OUTSIDE THE FRAMEWORK.
     * This could produce undesired results by not partitioning record properly.
     *
     * @see FixedKeyProcessor
     */
    public static <KIn, VIn> FixedKeyRecord<KIn, VIn> create(final Record<KIn, VIn> record) {
        return new FixedKeyRecord<>(
            record.key(),
            record.value(),
            record.timestamp(),
            record.headers()
        );
    }
}
