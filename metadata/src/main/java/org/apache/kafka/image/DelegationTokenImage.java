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

import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.DescribeDelegationTokenRequest;
import org.apache.kafka.common.message.DescribeDelegationTokenRequestData;
import org.apache.kafka.common.message.DescribeDelegationTokenRequestData.DescribeDelegationTokenOwner;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
// import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.metadata.DelegationTokenData;


import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
// import java.util.stream.Collectors;


/**
 * Represents the SCRAM credentials in the metadata image.
 *
 * This class is thread-safe.
 */
public final class DelegationTokenImage {
    public static final DelegationTokenImage EMPTY = new DelegationTokenImage(Collections.emptyMap());

    // Map TokenID to TokenInformation.
    // The TokenID is also contained in the TokenInformation inside the DelegationTokenData
    private final Map<String, DelegationTokenData> tokens;

    public DelegationTokenImage(Map<String, DelegationTokenData> tokens) {
        this.tokens = Collections.unmodifiableMap(tokens);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
// XXX Must do this       if (options.metadataVersion().isDelegationTokenSupported()) 
        for (Entry<String, DelegationTokenData> entry : tokens.entrySet()) {
            writer.write(0, entry.getValue().toRecord());
        }
    }

    private boolean filterOwners(RequestContext requestContext,
                                 List<KafkaPrincipal> owners,
                                 TokenInformation token) {
        for (KafkaPrincipal owner: owners) {
            if (token.ownerOrRenewer(owner)) {
                return true;
            }
        }
        return false;
    }

    /*
     * Return a list of TokenInformation for the requested owners or renewers.
     * or all if the list s null. Caller will authenticate for each TokenInformation
     */
    public List<TokenInformation> describe(RequestContext requestContext, 
        DescribeDelegationTokenRequestData describeDelegationTokenRequestData) {

        List<TokenInformation> infos = new ArrayList<TokenInformation>();
        List<KafkaPrincipal> owners = new ArrayList<KafkaPrincipal>();
        if (describeDelegationTokenRequestData.owners() != null) {
            for (DescribeDelegationTokenOwner owner: describeDelegationTokenRequestData.owners()) {
                owners.add(new KafkaPrincipal(owner.principalType(), owner.principalName()));
            }
        }

        for (DelegationTokenData tokenData : tokens.values()) {
            if ((describeDelegationTokenRequestData.owners() == null) ||
                filterOwners(requestContext, owners, tokenData.tokenInformation())) {
                infos.add(tokenData.tokenInformation());
            }
        }
        return infos;
    }

    public Map<String, DelegationTokenData> tokens() {
        return tokens;
    }

    public boolean isEmpty() {
        return tokens.isEmpty();
    }

    @Override
    public int hashCode() {
        return tokens.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!o.getClass().equals(DelegationTokenImage.class)) return false;
        DelegationTokenImage other = (DelegationTokenImage) o;
        return tokens.equals(other.tokens);
    }

    @Override
    public String toString() {
// XXX        return new DelegationTokenImageNode(this).stringify();
        return "";
    }
}
