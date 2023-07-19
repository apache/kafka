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
package org.apache.kafka.streams.processor.api;

/**
 * Processor context interface for {@link Record}.
 *
 * @param <KForward> a bound on the types of keys that may be forwarded
 * @param <VForward> a bound on the types of values that may be forwarded
 */
public interface ProcessorContext<KForward, VForward> extends ProcessingContext {
    /**
     * Forward a record to all child processors.
     * <p>
     * Note that the forwarded {@link Record} is shared between the parent and child
     * processors. And of course, the parent may forward the same object to multiple children,
     * and the child may forward it to grandchildren, etc. Therefore, you should be mindful
     * of mutability.
     * <p>
     * The {@link Record} class itself is immutable (all the setter-style methods return an
     * independent copy of the instance). However, the key, value, and headers referenced by
     * the Record may themselves be mutable.
     * <p>
     * Some programs may opt to make use of this mutability for high performance, in which case
     * the input record may be mutated and then forwarded by each {@link Processor}. However,
     * most applications should instead favor safety.
     * <p>
     * Forwarding records safely simply means to make a copy of the record before you mutate it.
     * This is trivial when using the {@link Record#withKey(Object)}, {@link Record#withValue(Object)},
     * and {@link Record#withTimestamp(long)} methods, as each of these methods make a copy of the
     * record as a matter of course. But a little extra care must be taken with headers, since
     * the {@link org.apache.kafka.common.header.Header} class is mutable. The easiest way to
     * safely handle headers is to use the {@link Record} constructors to make a copy before
     * modifying headers.
     * <p>
     * In other words, this would be considered unsafe:
     * <code>
     *     process(Record inputRecord) {
     *         inputRecord.headers().add(...);
     *         context.forward(inputRecord);
     *     }
     * </code>
     * This is unsafe because the parent, and potentially siblings, grandparents, etc.,
     * all will see this modification to their shared Headers reference. This is a violation
     * of causality and could lead to undefined behavior.
     * <p>
     * A safe usage would look like this:
     * <code>
     *     process(Record inputRecord) {
     *         // makes a copy of the headers
     *         Record toForward = inputRecord.withHeaders(inputRecord.headers());
     *         // Other options to create a safe copy are:
     *         // * use any copy-on-write method, which makes a copy of all fields:
     *         //   toForward = inputRecord.withValue();
     *         // * explicitly copy all fields:
     *         //   toForward = new Record(inputRecord.key(), inputRecord.value(), inputRecord.timestamp(), inputRecord.headers());
     *         // * create a fresh, empty Headers:
     *         //   toForward = new Record(inputRecord.key(), inputRecord.value(), inputRecord.timestamp());
     *         // * etc.
     *
     *         // now, we are modifying our own independent copy of the headers.
     *         toForward.headers().add(...);
     *         context.forward(toForward);
     *     }
     * </code>
     * @param record The record to forward to all children
     */
    <K extends KForward, V extends VForward> void forward(Record<K, V> record);

    /**
     * Forward a record to the specified child processor.
     * See {@link ProcessorContext#forward(Record)} for considerations.
     *
     * @param record The record to forward
     * @param childName The name of the child processor to receive the record
     * @see ProcessorContext#forward(Record)
     */
    <K extends KForward, V extends VForward> void forward(Record<K, V> record, final String childName);
}
