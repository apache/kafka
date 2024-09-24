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

/**
 * FieldDomain represents whether a field should appear in message1, message2, both, or neither.
 *
 * Note that this class does not handle tagged fields. Tagged fields can appear in any version,
 * provided that the version is a flexibleVersion. In other words, adding a tagged field to an
 * existing version is not an incompatible change. (However, reusing a tag index certainly is.)
 */
enum FieldDomain {
    MESSAGE1_ONLY,
    BOTH,
    MESSAGE2_ONLY,
    NEITHER;

    static FieldDomain of(
        FieldSpec fieldSpec,
        Versions versions1,
        Versions versions2
    ) {
        Versions intersection1 = versions1.intersect(fieldSpec.versions());
        Versions intersection2 = versions2.intersect(fieldSpec.versions());
        if (intersection1.empty()) {
            if (intersection2.empty()) {
                return NEITHER;
            } else {
                return MESSAGE2_ONLY;
            }
        } else if (intersection2.empty()) {
            return MESSAGE1_ONLY;
        } else {
            return BOTH;
        }
    }

    void validate(
        String what,
        FieldSpec field,
        boolean present1,
        boolean present2
    ) {
        switch (this) {
            case MESSAGE1_ONLY:
                if (present2) {
                    throw new UnificationException(what + " " + field.name() + " is present in " +
                            "message2, but should not be, based on its versions.");
                }
                if (!present1) {
                    throw new UnificationException(what + " " + field.name() + " is not present in " +
                            "message1, but should be, based on its versions.");
                }
                break;
            case BOTH:
                if (!present1) {
                    throw new UnificationException(what + " " + field.name() + " is not present in " +
                            "message1, but should be, based on its versions.");
                }
                if (!present2) {
                    throw new UnificationException(what + " " + field.name() + " is not present in " +
                            "message2, but should be, based on its versions.");
                }
                break;
            case MESSAGE2_ONLY:
                if (present1) {
                    throw new UnificationException(what + " " + field.name() + " is present in " +
                            "message1, but should not be, based on its versions.");
                }
                if (!present2) {
                    throw new UnificationException(what + " " + field.name() + " is not present in " +
                            "message2, but should be, based on its versions.");
                }
                break;
            case NEITHER:
                if (present1) {
                    throw new UnificationException(what + " " + field.name() + " is present in " +
                            "message1, but should not be, based on its versions.");
                }
                if (present2) {
                    throw new UnificationException(what + " " + field.name() + " is present in " +
                            "message2, but should not be, based on its versions.");
                }
                break;
        }
    }
}
