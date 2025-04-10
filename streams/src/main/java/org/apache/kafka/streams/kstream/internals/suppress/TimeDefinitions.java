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

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.RecordContext;

final class TimeDefinitions {
    private TimeDefinitions() {}

    /**
     * This interface should never be instantiated outside of this class.
     */
    interface TimeDefinition<K> {
        long time(final RecordContext context, final K key);
    }

    static class RecordTimeDefinition<K> implements TimeDefinition<K> {
        private static final RecordTimeDefinition<?> INSTANCE = new RecordTimeDefinition<>();

        private RecordTimeDefinition() {}

        @SuppressWarnings("unchecked")
        static <K> RecordTimeDefinition<K> instance() {
            return (RecordTimeDefinition<K>) RecordTimeDefinition.INSTANCE;
        }

        @Override
        public long time(final RecordContext context, final K key) {
            return context.timestamp();
        }
    }

    static class WindowEndTimeDefinition<K extends Windowed<?>> implements TimeDefinition<K> {
        private static final WindowEndTimeDefinition<?> INSTANCE = new WindowEndTimeDefinition<>();

        private WindowEndTimeDefinition() {}

        @SuppressWarnings("unchecked")
        static <K extends Windowed<?>> WindowEndTimeDefinition<K> instance() {
            return (WindowEndTimeDefinition<K>) WindowEndTimeDefinition.INSTANCE;
        }

        @Override
        public long time(final RecordContext context, final K key) {
            return key.window().end();
        }
    }
}
