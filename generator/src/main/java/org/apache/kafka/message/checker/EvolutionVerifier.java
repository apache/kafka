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
import org.apache.kafka.message.MessageSpec;
import org.apache.kafka.message.StructSpec;

public class EvolutionVerifier {
    private final MessageSpec topLevelMessage1;
    private final MessageSpec topLevelMessage2;

    public EvolutionVerifier(
        MessageSpec topLevelMessage1,
        MessageSpec topLevelMessage2
    ) {
        this.topLevelMessage1 = topLevelMessage1;
        this.topLevelMessage2 = topLevelMessage2;
    }

    public void verify() throws Exception {
        verifyTopLevelMessages(topLevelMessage1, topLevelMessage2);
        verifyVersionsMatchTopLevelMessage("message1", topLevelMessage1);
        verifyVersionsMatchTopLevelMessage("message2", topLevelMessage2);
        Unifier unifier = new Unifier(topLevelMessage1, topLevelMessage2);
        unifier.unify();
    }

    static void verifyTopLevelMessages(MessageSpec topLevelMessage1, MessageSpec topLevelMessage2) {
        if (!topLevelMessage1.apiKey().equals(topLevelMessage2.apiKey())) {
            throw new EvolutionException("Initial apiKey " + topLevelMessage1.apiKey() +
                " does not match final apiKey " + topLevelMessage2.apiKey());
        }
        if (!topLevelMessage1.type().equals(topLevelMessage2.type())) {
            throw new EvolutionException("Initial type " + topLevelMessage1.type() +
                " does not match final type " + topLevelMessage2.type());
        }
        if (!topLevelMessage2.flexibleVersions().contains(topLevelMessage1.flexibleVersions())) {
            throw new EvolutionException("Initial flexibleVersions " + topLevelMessage1.flexibleVersions() +
                " must be a subset of final flexibleVersions " + topLevelMessage2.flexibleVersions());
        }
        if (topLevelMessage2.validVersions().highest() < topLevelMessage1.validVersions().highest()) {
            throw new EvolutionException("Initial maximum valid version " +
                topLevelMessage1.validVersions().highest() + " must not be higher than final " +
                "maximum valid version " + topLevelMessage2.validVersions().highest());
        }
        if (topLevelMessage2.validVersions().lowest() < topLevelMessage1.validVersions().lowest()) {
            throw new EvolutionException("Initial minimum valid version " +
                topLevelMessage1.validVersions().lowest() + " must not be higher than final " +
                "minimum valid version " + topLevelMessage2.validVersions().lowest());
        }
    }

    static void verifyVersionsMatchTopLevelMessage(
        String what,
        MessageSpec topLevelMessage
    ) {
        for (FieldSpec field : topLevelMessage.fields()) {
            verifyVersionsMatchTopLevelMessage(what, topLevelMessage, field);
        }
        for (StructSpec struct : topLevelMessage.commonStructs()) {
            for (FieldSpec field : topLevelMessage.fields()) {
                verifyVersionsMatchTopLevelMessage(what, topLevelMessage, field);
            }
        }
    }

    static void verifyVersionsMatchTopLevelMessage(
        String what,
        MessageSpec topLevelMessage,
        FieldSpec field
    ) {
        if (topLevelMessage.validVersions().intersect(field.versions()).empty()) {
            throw new EvolutionException("Field " + field.name() + " in  " + what + " has versions " +
                field.versions() + ", but the message versions are only " +
                topLevelMessage.validVersions() + ".");
        }
        if (!field.nullableVersions().empty()) {
            if (topLevelMessage.validVersions().intersect(field.nullableVersions()).empty()) {
                throw new EvolutionException("Field " + field.name() + " in  " + what +
                    " has nullableVersions " + field.nullableVersions() + ", but the message " +
                    "versions are only " + topLevelMessage.validVersions() + ".");
            }
        }
        if (field.tag().isPresent()) {
            if (topLevelMessage.validVersions().intersect(field.taggedVersions()).empty()) {
                throw new EvolutionException("Field " + field.name() + " in  " + what +
                    " has taggedVersions " + field.taggedVersions() + ", but the message " +
                    "versions are only " + topLevelMessage.validVersions() + ".");
            }
        }
        field.flexibleVersions().ifPresent(v -> {
            if (topLevelMessage.validVersions().intersect(v).empty()) {
                throw new EvolutionException("Field " + field.name() + " in  " + what +
                    " has flexibleVersions " + v + ", but the message versions are only " +
                    topLevelMessage.validVersions() + ".");
            }

        });
        for (FieldSpec child : field.fields()) {
            verifyVersionsMatchTopLevelMessage(what, topLevelMessage, child);
        }
    }
}
