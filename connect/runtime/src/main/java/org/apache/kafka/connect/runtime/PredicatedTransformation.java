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

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;

/**
 * Decorator for a {@link Transformation} which applies the delegate only when a
 * {@link Predicate} is true (or false, according to {@code negate}).
 * @param <R>
 */
public class PredicatedTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

    static final String PREDICATE_CONFIG = "predicate";
    static final String NEGATE_CONFIG = "negate";
    final Predicate<R> predicate;
    final Transformation<R> delegate;
    final boolean negate;

    PredicatedTransformation(Predicate<R> predicate, boolean negate, Transformation<R> delegate) {
        this.predicate = predicate;
        this.negate = negate;
        this.delegate = delegate;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        throw new ConnectException(PredicatedTransformation.class.getName() + ".configure() " +
                "should never be called directly.");
    }

    @Override
    public R apply(R record) {
        if (negate ^ predicate.test(record)) {
            return delegate.apply(record);
        }
        return record;
    }

    @Override
    public ConfigDef config() {
        throw new ConnectException(PredicatedTransformation.class.getName() + ".config() " +
                "should never be called directly.");
    }

    @Override
    public void close() {
        Utils.closeQuietly(delegate, "predicated");
        Utils.closeQuietly(predicate, "predicate");
    }

    @Override
    public String toString() {
        return "PredicatedTransformation{" +
                "predicate=" + predicate +
                ", delegate=" + delegate +
                ", negate=" + negate +
                '}';
    }
}
