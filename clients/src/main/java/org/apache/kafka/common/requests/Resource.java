package org.apache.kafka.common.requests;

public final class Resource {
    private final ResourceType type;
    private final String name;

    public Resource(ResourceType type, String name) {
        this.type = type;
        this.name = name;
    }

    public ResourceType type() {
        return type;
    }

    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Resource resource = (Resource) o;

        return type == resource.type && name.equals(resource.name);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Resource(type=" + type + ", name='" + name + "'}";
    }
}
