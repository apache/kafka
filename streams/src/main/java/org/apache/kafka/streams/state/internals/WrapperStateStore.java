package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;

/**
 * A storage engine wrapper for utilities like logging, caching, and metering.
 */
interface WrapperStateStore extends StateStore {

    /**
     * Return the inner storage engine
     *
     * @return wrapped inner storage engine
     */
    StateStore inner();

    abstract class AbstractStateStore implements WrapperStateStore {
        protected final StateStore innerState;

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
            if (innerState instanceof WrapperKeyValueStore)
                return ((WrapperStateStore) innerState).inner();
            else
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
