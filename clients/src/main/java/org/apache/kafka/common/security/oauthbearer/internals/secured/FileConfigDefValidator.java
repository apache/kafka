package org.apache.kafka.common.security.oauthbearer.internals.secured;

import java.io.File;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class FileConfigDefValidator implements ConfigDef.Validator {

    private final boolean isRequired;

    public FileConfigDefValidator() {
        this(false);
    }

    public FileConfigDefValidator(boolean isRequired) {
        this.isRequired = isRequired;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value == null) {
            if (isRequired)
                throw new ConfigException(String.format("The OAuth configuration option %s is required", name));
            else
                return;
        }

        File file = new File(value.toString().trim());

        if (!file.exists())
            throw new ConfigException(String.format("The OAuth configuration option %s contains a file (%s) that doesn't exist", name, value));

        if (!file.canRead())
            throw new ConfigException(String.format("The OAuth configuration option %s contains a file (%s) that doesn't have read permission", name, value));
    }

}
