package com.microsoft.azpubsub.security.oauthbearer;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

/*
 * AzPubSub oAuth token model
 */
public class AzPubSubOAuthBearerToken implements OAuthBearerToken {
    private String value;
    private Set<String> scopes;
    private long lifetimeMs;
    private String principalName;
    private Long startTimeMs;

    public AzPubSubOAuthBearerToken(String accessToken, long lifetimeS, String principalName, Long startTimeMs) {
        super();
        this.value = accessToken;
        this.scopes = new LinkedHashSet<String>();
        this.lifetimeMs = lifetimeS;
        this.principalName= principalName;
        this.startTimeMs = startTimeMs;
    }

    @Override
    public String value() {
        return this.value;
    }

    @Override
    public Set<String> scope() {
        return this.scopes;
    }

    @Override
    public long lifetimeMs() {
        return this.lifetimeMs;
    }

    @Override
    public String principalName() {
        return this.principalName;
    }

    @Override
    public Long startTimeMs() {
        return this.startTimeMs;
    }

    public void addScope(String scope) {
        this.scopes.add(scope);
    }
}
