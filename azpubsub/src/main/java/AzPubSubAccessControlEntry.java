import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryData;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;

import java.util.Objects;

public class AzPubSubAccessControlEntry{

    AzPubSubAccessControlEntryData data;

    public AzPubSubAccessControlEntry(String principal, String host, AclOperation operation, AclPermissionType permissionType, String aggregatedOperation) {
        Objects.requireNonNull(principal);
        Objects.requireNonNull(host);
        Objects.requireNonNull(operation);
        //make operations ANY on producer
        Objects.requireNonNull(permissionType);
        if (permissionType == AclPermissionType.ANY)
            throw new IllegalArgumentException("permissionType must not be ANY");
        this.data = new AzPubSubAccessControlEntryData(principal, host, operation, permissionType, aggregatedOperation);
    }

    public String principal() {
        return data.principal();
    }

    public String host() {
        return data.host();
    }

    public AclOperation operation() {
        return data.operation();
    }

    public AclPermissionType permissionType() {
        return data.permissionType();
    }

    public String aggregatedOperation() {
        return data.aggregatedOperation();
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AzPubSubAccessControlEntry))
            return false;
        AzPubSubAccessControlEntry other = (AzPubSubAccessControlEntry) o;
        return data.equals(other.data);
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

}
