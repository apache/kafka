package org.apache.kafka.common.security.oauthbearer;

import java.util.Collections;
import java.util.Set;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

public class BasicOAuthBearerToken implements OAuthBearerToken {

    private final String value;

    private final Set<String> scope;

    private final Long lifetimeMs;

    private final String principalName;

    private final Long startTimeMs;

    public BasicOAuthBearerToken(final String value, final Set<String> scope, final Long lifetimeMs,
        final String principalName,
        final Long startTimeMs) {
        this.value = value;
        this.scope = Collections.unmodifiableSet(scope);
        this.lifetimeMs = lifetimeMs;
        this.principalName = principalName;
        this.startTimeMs = startTimeMs;
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public Set<String> scope() {
        return scope;
    }

    @Override
    public long lifetimeMs() {
        return lifetimeMs;
    }

    @Override
    public String principalName() {
        return principalName;
    }

    @Override
    public Long startTimeMs() {
        return startTimeMs;
    }

}
