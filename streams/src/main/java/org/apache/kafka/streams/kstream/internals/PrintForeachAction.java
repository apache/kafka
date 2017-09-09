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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.io.PrintWriter;

public class PrintForeachAction<K, V> implements ForeachAction<K, V> {

    private final String label;
    private final PrintWriter printWriter;
    private final KeyValueMapper<? super K, ? super V, String> mapper;
    /**
     * Print customized output with given writer. The PrintWriter can be null in order to
     * distinguish between {@code System.out} and the others. If the PrintWriter is {@code PrintWriter(System.out)},
     * then it would close {@code System.out} output stream.
     * <p>
     * Afterall, not to pass in {@code PrintWriter(System.out)} but {@code null} instead.
     *
     * @param printWriter Use {@code System.out.println} if {@code null}.
     * @param mapper The mapper which can allow user to customize output will be printed.
     * @param label The given name will be printed.
     */
    public PrintForeachAction(final PrintWriter printWriter, final KeyValueMapper<? super K, ? super V, String> mapper, final String label) {
        this.printWriter = printWriter;
        this.mapper = mapper;
        this.label = label;
    }

    @Override
    public void apply(final K key, final V value) {
        final String data = String.format("[%s]: %s", label, mapper.apply(key, value));
        if (printWriter == null) {
            System.out.println(data);
        } else {
            printWriter.println(data);
        }
    }

    public void close() {
        if (printWriter == null) {
            System.out.flush();
        } else {
            printWriter.close();
        }
    }

}
