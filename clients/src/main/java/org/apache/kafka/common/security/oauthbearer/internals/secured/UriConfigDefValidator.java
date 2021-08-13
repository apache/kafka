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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class UriConfigDefValidator implements ConfigDef.Validator {

    private final boolean isRequired;

    public UriConfigDefValidator() {
        this(false);
    }

    public UriConfigDefValidator(boolean isRequired) {
        this.isRequired = isRequired;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value == null) {
            if (isRequired)
                throw new ConfigException(String.format("The OAuth configuration option %s is required", name));
            else
                return;
        }

        try {
            new URI(value.toString());
        } catch (URISyntaxException e) {
            throw new ConfigException(String.format("The OAuth configuration option %s contains a URI (%s) that is malformed", name, value));
        }
    }

}
