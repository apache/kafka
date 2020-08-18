import org.apache.kafka.common.acl.AccessControlEntryData;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;

import java.util.Objects;


public class AzPubSubAccessControlEntryData extends AccessControlEntryData {
    private String aggregatedOperation;

    AzPubSubAccessControlEntryData(String principal, String host, AclOperation operation, AclPermissionType permissionType, String aggregatedOperation) {
       super(principal, host, operation, permissionType);
       this.aggregatedOperation = aggregatedOperation;
    }

    String aggregatedOperation() {
        return aggregatedOperation;
    }

    String principal() {
        return principal;
    }

    String host() {
        return host;
    }

    AclOperation operation() {
        return operation;
    }

    AclPermissionType permissionType() {
        return permissionType;
    }

    @Override
    public String toString() {
        return "(" + super.toString().trim().replaceAll("\\)$|^\\(", "") + ", aggregatedOperation=" + aggregatedOperation + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AccessControlEntryData))
            return false;
        AzPubSubAccessControlEntryData other = (AzPubSubAccessControlEntryData) o;
        return super.equals(o) && Objects.equals(aggregatedOperation, other.aggregatedOperation);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hash(aggregatedOperation);
    }

}
