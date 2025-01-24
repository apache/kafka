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

package org.apache.kafka.message.checker;

import org.apache.kafka.message.FieldSpec;
import org.apache.kafka.message.Versions;

import java.util.Iterator;
import java.util.NoSuchElementException;

class FieldSpecPairIterator implements Iterator<FieldSpecPair> {
    private final Iterator<FieldSpec> iterator1;
    private final Iterator<FieldSpec> iterator2;
    private final Versions topLevelVersions1;
    private final Versions topLevelVersions2;
    private FieldSpecPair next;

    FieldSpecPairIterator(
        Iterator<FieldSpec> iterator1,
        Iterator<FieldSpec> iterator2,
        Versions topLevelVersions1,
        Versions topLevelVersions2
    ) {
        this.iterator1 = iterator1;
        this.iterator2 = iterator2;
        this.topLevelVersions1 = topLevelVersions1;
        this.topLevelVersions2 = topLevelVersions2;
        this.next = null;
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        FieldSpec field1 = iterator1.hasNext() ? iterator1.next() : null;
        while (field1 != null) {
            FieldDomain domain1 = FieldDomain.of(field1, topLevelVersions1, topLevelVersions2);
            switch (domain1) {
                case MESSAGE1_ONLY:
                    field1 = iterator1.hasNext() ? iterator1.next() : null;
                    break;
                case BOTH: {
                    FieldSpec field2 = iterator2.hasNext() ? iterator2.next() : null;
                    while (field2 != null) {
                        FieldDomain domain2 = FieldDomain.of(field2, topLevelVersions1, topLevelVersions2);
                        switch (domain2) {
                            case MESSAGE2_ONLY:
                                field2 = iterator2.hasNext() ? iterator2.next() : null;
                                break;
                            case BOTH:
                                next = new FieldSpecPair(field1, field2);
                                return true;
                            case MESSAGE1_ONLY:
                            case NEITHER:
                                throw new UnificationException("field2 " + field2.name() + " is present in " +
                                    "message2, but should not be, based on its versions.");
                        }
                    }
                    break;
                }
                case MESSAGE2_ONLY:
                case NEITHER:
                    throw new UnificationException("field1 " + field1.name() + " is present in " +
                        "message1, but should not be, based on its versions.");
            }
        }
        FieldSpec field2 = iterator2.hasNext() ? iterator2.next() : null;
        while (field2 != null) {
            FieldDomain domain2 = FieldDomain.of(field2, topLevelVersions1, topLevelVersions2);
            switch (domain2) {
                case MESSAGE1_ONLY:
                case NEITHER:
                    throw new UnificationException("field2 " + field2.name() + " is present in " +
                        "message2, but should not be, based on its versions.");
                case BOTH:
                    throw new UnificationException("field2 " + field2.name() + " should be present " +
                        "in message1, but is not, based on its versions.");
                case MESSAGE2_ONLY:
                    field2 = iterator2.hasNext() ? iterator2.next() : null;
                    break;
            }
        }
        return false;
    }

    @Override
    public FieldSpecPair next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        FieldSpecPair result = next;
        next = null;
        return result;
    }
}
