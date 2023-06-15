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
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Represents a chain of {@link Transformation}s to be applied to a {@link ConnectRecord} serially.
 * @param <R> The type of record (must be an implementation of {@link ConnectRecord})
 */
public class TransformationChain<R extends ConnectRecord<R>> implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TransformationChain.class);

    private final List<TransformationStage<R>> transformationStages;
    private final RetryWithToleranceOperator retryWithToleranceOperator;

    public TransformationChain(List<TransformationStage<R>> transformationStages, RetryWithToleranceOperator retryWithToleranceOperator) {
        this.transformationStages = transformationStages;
        this.retryWithToleranceOperator = retryWithToleranceOperator;
    }

    public R apply(R record) {
        if (transformationStages.isEmpty()) return record;

        for (final TransformationStage<R> transformationStage : transformationStages) {
            final R current = record;

            log.trace("Applying transformation {} to {}",
                transformationStage.transformClass().getName(), record);
            // execute the operation
            record = retryWithToleranceOperator.execute(() -> transformationStage.apply(current), Stage.TRANSFORMATION, transformationStage.transformClass());

            if (record == null) break;
        }

        return record;
    }

    @Override
    public void close() {
        for (TransformationStage<R> transformationStage : transformationStages) {
            transformationStage.close();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransformationChain<?> that = (TransformationChain<?>) o;
        return Objects.equals(transformationStages, that.transformationStages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformationStages);
    }

    public String toString() {
        StringJoiner chain = new StringJoiner(", ", getClass().getName() + "{", "}");
        for (TransformationStage<R> transformationStage : transformationStages) {
            chain.add(transformationStage.transformClass().getName());
        }
        return chain.toString();
    }
}
