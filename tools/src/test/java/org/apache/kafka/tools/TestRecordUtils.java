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
package org.apache.kafka.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

public class TestRecordUtils {
    private static final int READ_AHEAD_LIMIT = 4906;

    public static TestRecord readNext(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return null;
        }
        TestRecord curr = TestRecord.parse(line);
        while (true) {
            String peekedLine = peekLine(reader);
            if (peekedLine == null) {
                return curr;
            }
            TestRecord next = TestRecord.parse(peekedLine);
            if (!next.getTopicAndKey().equals(curr.getTopicAndKey())) {
                return curr;
            }
            curr = next;
            reader.readLine();
        }
    }

    public static Iterator<TestRecord> valuesIterator(BufferedReader reader) {
        return Spliterators.iterator(new Spliterators.AbstractSpliterator<>(
                Long.MAX_VALUE, Spliterator.ORDERED) {
            @Override
            public boolean tryAdvance(Consumer<? super TestRecord> action) {
                try {
                    TestRecord rec;
                    do {
                        rec = readNext(reader);
                    } while (rec != null && rec.delete);
                    if (rec == null) return false;
                    action.accept(rec);
                    return true;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    public static String peekLine(BufferedReader reader) throws IOException {
        reader.mark(READ_AHEAD_LIMIT);
        String line = reader.readLine();
        reader.reset();
        return line;
    }
}
