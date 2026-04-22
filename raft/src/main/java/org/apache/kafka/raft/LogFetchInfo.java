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
package org.apache.kafka.raft;

import org.apache.kafka.common.record.internal.Records;

/**
 * The class is not converted to a Java record since records are typically intended to be immutable, but this one contains a mutable field records
 */
public class LogFetchInfo {

    public final Records records;
    public final LogOffsetMetadata startOffsetMetadata;

    public LogFetchInfo(Records records, LogOffsetMetadata startOffsetMetadata) {
        this.records = records;
        this.startOffsetMetadata = startOffsetMetadata;
    }
}
