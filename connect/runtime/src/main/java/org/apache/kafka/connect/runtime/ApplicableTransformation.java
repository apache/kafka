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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ApplicableTransformation<R extends ConnectRecord<R>> implements Closeable {

    private final ApplicableTransformationConfig config;
    protected Transformation<R> transformation;

    <T extends Transformation<R>> ApplicableTransformation(final T transformation,
                                                           final Map<String, Object> config) {
        this.transformation = transformation;
        this.config = new ApplicableTransformationConfig(config);
    }

    <T extends Transformation<R>> ApplicableTransformation(final T transformation) {
        this.transformation = transformation;
        this.config = new ApplicableTransformationConfig(Collections.emptyMap());
    }

    R apply(final R record) {
        final List<String> topics = config.getTopics();
        if (topics.isEmpty() || !topics.contains(record.topic())) {
            return record;
        }

        return transformation.apply(record);
    }

    Transformation<R> getTransformation() {
        return transformation;
    }

    @Override
    public void close() {
        transformation.close();
    }
}
