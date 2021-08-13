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
import java.util.Locale;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class UriConfigDefValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value == null || value.toString().trim().isEmpty())
            return;

        try {
            URI uri = new URI(value.toString().trim());
            String scheme = uri.getScheme();

            if (scheme == null || scheme.trim().isEmpty())
                throw new ConfigException(String.format("The OAuth configuration option %s contains a URI (%s) that is missing the scheme", name, value));

            scheme = scheme.toLowerCase(Locale.ENGLISH);

            if (!(scheme.equals("http") || scheme.equals("https")))
                throw new ConfigException(String.format("The OAuth configuration option %s contains a URI (%s) that contains an invalid scheme (%s); only \"http\" and \"https\" schemes are supported", name, value, scheme));
        } catch (URISyntaxException e) {
            throw new ConfigException(String.format("The OAuth configuration option %s contains a URI (%s) that is malformed: %s", name, value, e.getMessage()));
        }
    }

}
