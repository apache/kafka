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

package org.apache.kafka.common.security.oauthbearer.secured;

import java.security.Key;
import java.util.List;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.jose4j.lang.UnresolvableKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RefreshingHttpsJwksVerificationKeyResolver implements CloseableVerificationKeyResolver {

    private static final Logger log = LoggerFactory.getLogger(RefreshingHttpsJwksVerificationKeyResolver.class);

    private RefreshingHttpsJwks httpsJkws;

    private VerificationKeyResolver delegate;

    public RefreshingHttpsJwksVerificationKeyResolver(String location, long refreshIntervalMs) {
        this.httpsJkws = new RefreshingHttpsJwks(location, refreshIntervalMs);
        this.delegate = new HttpsJwksVerificationKeyResolver(httpsJkws);
    }

    @Override
    public void init() {
        try {
            log.debug("initialization started");

            httpsJkws.init();
        } finally {
            log.debug("initialization completed");
        }
    }

    @Override
    public void close() {
        try {
            log.debug("close started");

            httpsJkws.close();
        } finally {
            log.debug("close completed");
        }
    }

    @Override
    public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
        return delegate.resolveKey(jws, nestingContext);
    }

}
