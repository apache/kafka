package org.apache.kafka.common.security.oauthbearer.internals.secured;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class UriConfigDefValidator implements ConfigDef.Validator {

    private final String mode;

    private final boolean isRequired;

    public UriConfigDefValidator(String mode, boolean isRequired) {
        this.mode = mode;
        this.isRequired = isRequired;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value == null) {
            if (isRequired)
                throw new ConfigException(String.format("The OAuth %s configuration option %s is required", mode, name));
            else
                return;
        }

        try {
            new URI(value.toString());
        } catch (URISyntaxException e) {
            throw new ConfigException(String.format("The OAuth %s configuration option %s contains a URI (%s) that is malformed", mode, name, value));
        }
    }

}
