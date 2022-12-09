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

package org.apache.kafka.image;

import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.clients.admin.ScramMechanism;

// import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
// import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * Represents the SCRAM credentials in the metadata image.
 *
 * This class is thread-safe.
 */
public final class ScramImage {
    public static final ScramImage EMPTY = new ScramImage(Collections.emptyMap());

    private final Map<ScramMechanism, Map<String, ScramCredentialData>> mechanisms;

    public ScramImage(Map<ScramMechanism, Map<String, ScramCredentialData>> mechanisms) {
        this.mechanisms = Collections.unmodifiableMap(mechanisms);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        for (Entry<ScramMechanism, Map<String, ScramCredentialData>> mechanismEntry : mechanisms.entrySet()) {
            for (Entry<String, ScramCredentialData> userEntry : mechanismEntry.getValue().entrySet()) {
                writer.write(0, userEntry.getValue().toRecord(userEntry.getKey(), mechanismEntry.getKey()));
            }
        }
    }

//    public void write(Consumer<List<ApiMessageAndVersion>> out) {
//        List<ApiMessageAndVersion> batch = new ArrayList<>();
//        for (Entry<ScramMechanism, Map<String, ScramCredentialData>> mechanismEntry : mechanisms.entrySet()) {
//            for (Entry<String, ScramCredentialData> userEntry : mechanismEntry.getValue().entrySet()) {
//                batch.add(new ApiMessageAndVersion(userEntry.getValue().toRecord(userEntry.getKey(), mechanismEntry.getKey()), (short) 0));
//            }
//        }
//        out.accept(batch);
//    }

    public Map<ScramMechanism, Map<String, ScramCredentialData>> mechanisms() {
        return mechanisms;
    }

    public boolean isEmpty() {
        return mechanisms.isEmpty();
    }

    @Override
    public int hashCode() {
        return mechanisms.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!o.getClass().equals(ScramImage.class)) return false;
        ScramImage other = (ScramImage) o;
        return mechanisms.equals(other.mechanisms);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ScramImage(");
        List<ScramMechanism> sortedMechanisms = mechanisms.keySet().stream().sorted().collect(Collectors.toList());
        String preMechanismComma = "";
        for (ScramMechanism mechanism : sortedMechanisms) {
            builder.append(preMechanismComma).append(mechanism).append(": {");
            Map<String, ScramCredentialData> userMap = mechanisms.get(mechanism);
            List<String> sortedUserNames = userMap.keySet().stream().sorted().collect(Collectors.toList());
            String preUserNameComma = "";
            for (String userName : sortedUserNames) {
                builder.append(preUserNameComma).append(userName).append("=").append(userMap.get(userName));
                preUserNameComma = ", ";
            }
            builder.append("}");
            preMechanismComma = ", ";
        }
        builder.append(")");
        return builder.toString();
    }
}
