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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.jose4j.keys.resolvers.VerificationKeyResolver;

public class AccessTokenValidatorFactory {

    public static AccessTokenValidator create(LoginCallbackHandlerConfiguration conf) {
        return new LoginAccessTokenValidator(conf.getScopeClaimName(), conf.getSubClaimName());
    }

    public static AccessTokenValidator create(ValidatorCallbackHandlerConfiguration conf, VerificationKeyResolver verificationKeyResolver) {
        List<String> expectedAudiences = conf.getExpectedAudiences();
        Integer clockSkew = conf.getClockSkew();
        String expectedIssuer = conf.getExpectedIssuer();
        String scopeClaimName = conf.getScopeClaimName();
        String subClaimName = conf.getSubClaimName();

        return new ValidatorAccessTokenValidator(clockSkew,
            expectedAudiences != null ? Collections.unmodifiableSet(new HashSet<>(expectedAudiences)) : null,
            expectedIssuer,
            verificationKeyResolver,
            scopeClaimName,
            subClaimName);
    }

}
