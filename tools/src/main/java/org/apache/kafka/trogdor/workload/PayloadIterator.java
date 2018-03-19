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

package org.apache.kafka.trogdor.workload;

import java.util.Iterator;

/**
 * An iterator which wraps a PayloadGenerator.
 */
public final class PayloadIterator implements Iterator<byte[]> {
    private final PayloadGenerator generator;
    private long position = 0;

    public PayloadIterator(PayloadGenerator generator) {
        this.generator = generator;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public synchronized byte[] next() {
        return generator.generate(position++);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public synchronized void seek(long position) {
        this.position = position;
    }

    public synchronized long position() {
        return this.position;
    }
}
