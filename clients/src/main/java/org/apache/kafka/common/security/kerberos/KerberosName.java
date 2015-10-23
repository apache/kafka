/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.kerberos;

import java.io.IOException;
import java.util.List;

public class KerberosName {

    /** The first component of the name */
    private final String serviceName;
    /** The second component of the name. It may be null. */
    private final String hostName;
    /** The realm of the name. */
    private final String realm;

    /* Rules for the translation of the principal name into an operating system name */
    private final List<KerberosRule> authToLocalRules;

    /**
     * Creates an instance of `KerberosName` with the provided parameters.
     */
    public KerberosName(String serviceName, String hostName, String realm, List<KerberosRule> authToLocalRules) {
        if (serviceName == null)
            throw new IllegalArgumentException("serviceName must not be null");
        this.serviceName = serviceName;
        this.hostName = hostName;
        this.realm = realm;
        this.authToLocalRules = authToLocalRules;
    }

    /**
     * Put the name back together from the parts.
     */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(serviceName);
        if (hostName != null) {
            result.append('/');
            result.append(hostName);
        }
        if (realm != null) {
            result.append('@');
            result.append(realm);
        }
        return result.toString();
    }

    /**
     * Get the first component of the name.
     * @return the first section of the Kerberos principal name
     */
    public String serviceName() {
        return serviceName;
    }

    /**
     * Get the second component of the name.
     * @return the second section of the Kerberos principal name, and may be null
     */
    public String hostName() {
        return hostName;
    }

    /**
     * Get the realm of the name.
     * @return the realm of the name, may be null
     */
    public String realm() {
        return realm;
    }

    /**
     * Get the translation of the principal name into an operating system
     * user name.
     * @return the short name
     * @throws IOException
     */
    public String shortName() throws IOException {
        String[] params;
        if (hostName == null) {
            // if it is already simple, just return it
            if (realm == null)
                return serviceName;
            params = new String[]{realm, serviceName};
        } else {
            params = new String[]{realm, serviceName, hostName};
        }
        for (KerberosRule r : authToLocalRules) {
            String result = r.apply(params);
            if (result != null)
                return result;
        }
        throw new NoMatchingRule("No rules applied to " + toString());
    }

}
