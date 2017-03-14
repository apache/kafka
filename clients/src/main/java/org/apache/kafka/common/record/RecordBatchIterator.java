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
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.AbstractIterator;

import java.io.IOException;

class RecordBatchIterator<T extends RecordBatch> extends AbstractIterator<T> {

    private final LogInputStream<T> logInputStream;

    RecordBatchIterator(LogInputStream<T> logInputStream) {
        this.logInputStream = logInputStream;
    }

    @Override
    protected T makeNext() {
        try {
            T batch = logInputStream.nextBatch();
            if (batch == null)
                return allDone();
            return batch;
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }
}
