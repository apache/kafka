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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Duration;
import java.util.Objects;

public class FinalResultsSuppressionBuilder<K extends Windowed> implements Suppressed<K> {
    private final StrictBufferConfig bufferConfig;

    public FinalResultsSuppressionBuilder(final Suppressed.StrictBufferConfig bufferConfig) {
        this.bufferConfig = bufferConfig;
    }

    public SuppressedImpl<K> buildFinalResultsSuppression(final Duration gracePeriod) {
        return new SuppressedImpl<>(
            gracePeriod,
            bufferConfig,
            (ProcessorContext context, K key) -> key.window().end()
        );
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FinalResultsSuppressionBuilder<?> that = (FinalResultsSuppressionBuilder<?>) o;
        return Objects.equals(bufferConfig, that.bufferConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bufferConfig);
    }

    @Override
    public String toString() {
        return "FinalResultsSuppressionBuilder{bufferConfig=" + bufferConfig + '}';
    }
}
