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


import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;

/**
 * Wrapper for a {@link Transformation} and corresponding optional {@link Predicate}
 * which applies the transformation when the {@link Predicate} is true (or false, according to {@code negate}).
 * If no {@link Predicate} is provided, the transformation will be unconditionally applied.
 * @param <R> The type of record (must be an implementation of {@link ConnectRecord})
 */
public class TransformationStage<R extends ConnectRecord<R>> implements AutoCloseable {

    static final String PREDICATE_CONFIG = "predicate";
    static final String NEGATE_CONFIG = "negate";
    private final Predicate<R> predicate;
    private final Transformation<R> transformation;
    private final boolean negate;

    TransformationStage(Transformation<R> transformation) {
        this(null, false, transformation);
    }

    TransformationStage(Predicate<R> predicate, boolean negate, Transformation<R> transformation) {
        this.predicate = predicate;
        this.negate = negate;
        this.transformation = transformation;
    }

    public Class<? extends Transformation<R>> transformClass() {
        @SuppressWarnings("unchecked")
        Class<? extends Transformation<R>> transformClass = (Class<? extends Transformation<R>>) transformation.getClass();
        return transformClass;
    }

    public R apply(R record) {
        if (predicate == null || negate ^ predicate.test(record)) {
            return transformation.apply(record);
        }
        return record;
    }

    @Override
    public void close() {
        Utils.closeQuietly(transformation, "transformation");
        Utils.closeQuietly(predicate, "predicate");
    }

    @Override
    public String toString() {
        return "TransformationStage{" +
                "predicate=" + predicate +
                ", transformation=" + transformation +
                ", negate=" + negate +
                '}';
    }
}
