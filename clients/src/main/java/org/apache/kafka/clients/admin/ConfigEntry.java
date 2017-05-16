package org.apache.kafka.clients.admin;

public class ConfigEntry {

    private final String name;
    private final String value;
    private final boolean isDefault;
    private final boolean isSensitive;
    private final boolean isReadOnly;

    public ConfigEntry(String name, String value) {
        this(name, value, false, false, false);
    }

    public ConfigEntry(String name, String value, boolean isDefault, boolean isSensitive, boolean isReadOnly) {
        this.name = name;
        this.value = value;
        this.isDefault = isDefault;
        this.isSensitive = isSensitive;
        this.isReadOnly = isReadOnly;
    }

    public String name() {
        return name;
    }

    public String value() {
        return value;
    }

    public boolean isDefault() {
        return isDefault;
    }

    public boolean isSensitive() {
        return isSensitive;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }
}
