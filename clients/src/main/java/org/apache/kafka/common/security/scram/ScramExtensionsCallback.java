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
package org.apache.kafka.common.security.scram;

import javax.security.auth.callback.Callback;
import java.util.Collections;
import java.util.Map;


/**
 * Optional callback used for SCRAM mechanisms if any extensions need to be set
 * in the SASL/SCRAM exchange.
 */
public class ScramExtensionsCallback implements Callback {
    private Map<String, String> extensions = Collections.emptyMap();

    /**
     * Returns map of the extension names and values that are sent by the client to
     * the server in the initial client SCRAM authentication message.
     * Default is an empty unmodifiable map.
     */
    public Map<String, String> extensions() {
        return extensions;
    }

    /**
     * Sets the SCRAM extensions on this callback. Maps passed in should be unmodifiable
     */
    public void extensions(Map<String, String> extensions) {
        this.extensions = extensions;
    }
}
