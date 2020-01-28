package com.microsoft.azpubsub.security.auth;

import java.util.Set;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

/*
 * AzPubSub Principal holding role
 */
public class AzPubSubPrincipal extends KafkaPrincipal {
    public static final String CERTIFICATE_TYPE = "Certificate";
    public static final String ROLE_TYPE = "Role";

    private Set<String> roles;

    public AzPubSubPrincipal(String principalType, String name, Set<String> roles) {
        super(principalType, name);
        this.roles = roles;
    }

    public Set<String> getRoles() {
        return this.roles;
    }
}
