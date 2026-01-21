package org.apache.kafka.publicapi;

/**
 * Represents a violation of the public API rules.
 */
public class PublicApiViolation {
    private final String className;
    private final String violationType;
    private final String description;
    private final String memberName;

    public PublicApiViolation(String className, String violationType, String description, String memberName) {
        this.className = className;
        this.violationType = violationType;
        this.description = description;
        this.memberName = memberName;
    }

    public String getClassName() {
        return className;
    }

    public String getViolationType() {
        return violationType;
    }

    public String getDescription() {
        return description;
    }

    public String getMemberName() {
        return memberName;
    }

    @Override
    public String toString() {
        if (memberName != null && !memberName.isEmpty()) {
            return String.format("[%s] %s.%s: %s", violationType, className, memberName, description);
        } else {
            return String.format("[%s] %s: %s", violationType, className, description);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PublicApiViolation that = (PublicApiViolation) o;

        if (!className.equals(that.className)) return false;
        if (!violationType.equals(that.violationType)) return false;
        if (!description.equals(that.description)) return false;
        return memberName != null ? memberName.equals(that.memberName) : that.memberName == null;
    }

    @Override
    public int hashCode() {
        int result = className.hashCode();
        result = 31 * result + violationType.hashCode();
        result = 31 * result + description.hashCode();
        result = 31 * result + (memberName != null ? memberName.hashCode() : 0);
        return result;
    }
}