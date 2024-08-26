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
package org.apache.kafka.common.utils;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Provides a flattened iterator over the inner elements of an outer iterator.
 */
public final class FlattenedIterator<O, I> extends AbstractIterator<I> {
    private final Iterator<O> outerIterator;
    private final Function<O, Iterator<I>> innerIteratorFunction;
    private Iterator<I> innerIterator;

    public FlattenedIterator(Iterator<O> outerIterator, Function<O, Iterator<I>> innerIteratorFunction) {
        this.outerIterator = outerIterator;
        this.innerIteratorFunction = innerIteratorFunction;
    }

    @Override
    protected I makeNext() {
        while (innerIterator == null || !innerIterator.hasNext()) {
            if (outerIterator.hasNext())
                innerIterator = innerIteratorFunction.apply(outerIterator.next());
            else
                return allDone();
        }
        return innerIterator.next();
    }
}
