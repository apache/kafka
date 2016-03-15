/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.util;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;

import java.util.Arrays;

public class ByteArrayProducerRecordEquals implements IArgumentMatcher {
    private ProducerRecord<byte[], byte[]> record;

    public static ProducerRecord<byte[], byte[]> eqProducerRecord(ProducerRecord<byte[], byte[]> in) {
        EasyMock.reportMatcher(new ByteArrayProducerRecordEquals(in));
        return null;
    }

    public ByteArrayProducerRecordEquals(ProducerRecord<byte[], byte[]> record) {
        this.record = record;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean matches(Object argument) {
        if (!(argument instanceof ProducerRecord))
            return false;
        ProducerRecord<byte[], byte[]> other = (ProducerRecord<byte[], byte[]>) argument;
        return record.topic().equals(other.topic()) &&
                record.partition() != null ? record.partition().equals(other.partition()) : other.partition() == null &&
                record.key() != null ? Arrays.equals(record.key(), other.key()) : other.key() == null &&
                record.value() != null ? Arrays.equals(record.value(), other.value()) : other.value() == null;
    }

    @Override
    public void appendTo(StringBuffer buffer) {
        buffer.append(record.toString());
    }
}
