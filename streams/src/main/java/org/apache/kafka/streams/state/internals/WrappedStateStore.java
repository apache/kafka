/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;

/**
 * A storage engine wrapper for utilities like logging, caching, and metering.
 */
interface WrappedStateStore extends StateStore {

    /**
     * Return the inner storage engine
     *
     * @return wrapped inner storage engine
     */
    StateStore inner();

    abstract class AbstractStateStore implements WrappedStateStore {
        final StateStore innerState;

        AbstractStateStore(StateStore inner) {
            this.innerState = inner;
        }

        @Override
        public void init(ProcessorContext context, StateStore root) {
            innerState.init(context, root);
        }

        @Override
        public String name() {
            return innerState.name();
        }

        @Override
        public boolean persistent() {
            return innerState.persistent();
        }

        @Override
        public boolean isOpen() {
            return innerState.isOpen();
        }

        void validateStoreOpen() {
            if (!innerState.isOpen()) {
                throw new InvalidStateStoreException("Store " + innerState.name() + " is currently closed.");
            }
        }

        @Override
        public StateStore inner() {
            if (innerState instanceof WrappedStateStore) {
                return ((WrappedStateStore) innerState).inner();
            }
            return innerState;
        }

        @Override
        public void flush() {
            innerState.flush();
        }

        @Override
        public void close() {
            innerState.close();
        }
    }
}
