package org.apache.kafka.connect.runtime.isolation;

import org.apache.kafka.connect.errors.ConnectException;

public class VersionedPluginLoadingException extends ConnectException {
    public VersionedPluginLoadingException(String message) {
        super(message);
    }

    public VersionedPluginLoadingException(String message, Throwable cause) {
        super(message, cause);
    }
}
