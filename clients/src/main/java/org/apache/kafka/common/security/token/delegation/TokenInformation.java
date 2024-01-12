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
package org.apache.kafka.common.security.token.delegation;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

/**
 * A class representing a delegation token details.
 *
 */
@InterfaceStability.Evolving
public class TokenInformation {

    private final KafkaPrincipal owner;
    private final KafkaPrincipal tokenRequester;
    private final Collection<KafkaPrincipal> renewers;
    private final long issueTimestamp;
    private final long maxTimestamp;
    private long expiryTimestamp;
    private final String tokenId;

    public TokenInformation(String tokenId, KafkaPrincipal owner,
                            Collection<KafkaPrincipal> renewers, long issueTimestamp, long maxTimestamp, long expiryTimestamp) {
        this(tokenId, owner, owner, renewers, issueTimestamp, maxTimestamp, expiryTimestamp);
    }

    public TokenInformation(String tokenId, KafkaPrincipal owner, KafkaPrincipal tokenRequester,
                            Collection<KafkaPrincipal> renewers, long issueTimestamp, long maxTimestamp, long expiryTimestamp) {
        this.tokenId = tokenId;
        this.owner = owner;
        this.tokenRequester = tokenRequester;
        this.renewers = renewers;
        this.issueTimestamp =  issueTimestamp;
        this.maxTimestamp =  maxTimestamp;
        this.expiryTimestamp =  expiryTimestamp;
    }

    // Convert record elements into a TokenInformation
    public static TokenInformation fromRecord(String tokenId, KafkaPrincipal owner, KafkaPrincipal tokenRequester,
                            Collection<KafkaPrincipal> renewers, long issueTimestamp, long maxTimestamp, long expiryTimestamp) {
        return new TokenInformation(
            tokenId, owner, tokenRequester, renewers, issueTimestamp, maxTimestamp, expiryTimestamp);
    }

    public KafkaPrincipal owner() {
        return owner;
    }

    public String ownerAsString() {
        return owner.toString();
    }

    public KafkaPrincipal tokenRequester() {
        return tokenRequester;
    }

    public String tokenRequesterAsString() {
        return tokenRequester.toString();
    }

    public Collection<KafkaPrincipal> renewers() {
        return renewers;
    }

    public Collection<String> renewersAsString() {
        Collection<String> renewerList = new ArrayList<>();
        for (KafkaPrincipal renewer : renewers) {
            renewerList.add(renewer.toString());
        }
        return renewerList;
    }

    public long issueTimestamp() {
        return issueTimestamp;
    }

    public long expiryTimestamp() {
        return expiryTimestamp;
    }

    public void setExpiryTimestamp(long expiryTimestamp) {
        this.expiryTimestamp = expiryTimestamp;
    }

    public String tokenId() {
        return tokenId;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public boolean ownerOrRenewer(KafkaPrincipal principal) {
        return owner.equals(principal) || tokenRequester.equals(principal) || renewers.contains(principal);
    }

    @Override
    public String toString() {
        return "TokenInformation{" +
            "owner=" + owner +
            ", tokenRequester=" + tokenRequester +
            ", renewers=" + renewers +
            ", issueTimestamp=" + issueTimestamp +
            ", maxTimestamp=" + maxTimestamp +
            ", expiryTimestamp=" + expiryTimestamp +
            ", tokenId='" + tokenId + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TokenInformation that = (TokenInformation) o;

        return issueTimestamp == that.issueTimestamp &&
            maxTimestamp == that.maxTimestamp &&
            Objects.equals(owner, that.owner) &&
            Objects.equals(tokenRequester, that.tokenRequester) &&
            Objects.equals(renewers, that.renewers) &&
            Objects.equals(tokenId, that.tokenId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(owner, tokenRequester, renewers, issueTimestamp, maxTimestamp, expiryTimestamp, tokenId);
    }
}
