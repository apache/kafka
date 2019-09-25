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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.utils.AbstractIterator;

import java.util.Iterator;
import java.util.List;

abstract class NestedIterator<O, I> extends AbstractIterator<I> {
    private final Iterator<O> outerIterator;
    private Iterator<I> innerIterator;

    NestedIterator(List<O> topicStates) {
        outerIterator = topicStates.iterator();
    }

    public abstract Iterable<I> innerIterable(O outer);

    @Override
    public I makeNext() {
        while (innerIterator == null || !innerIterator.hasNext()) {
            if (outerIterator.hasNext())
                innerIterator = innerIterable(outerIterator.next()).iterator();
            else
                return allDone();
        }
        return innerIterator.next();
    }
}
