package kafka.common.record;

/**
 * The compression type to use
 */
public enum CompressionType {
    NONE(0, "none"), GZIP(1, "gzip"), SNAPPY(2, "snappy");

    public final int id;
    public final String name;

    private CompressionType(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public static CompressionType forId(int id) {
        switch (id) {
            case 0:
                return NONE;
            case 1:
                return GZIP;
            case 2:
                return SNAPPY;
            default:
                throw new IllegalArgumentException("Unknown compression type id: " + id);
        }
    }

    public static CompressionType forName(String name) {
        if (NONE.name.equals(name))
            return NONE;
        else if (GZIP.name.equals(name))
            return GZIP;
        else if (SNAPPY.name.equals(name))
            return SNAPPY;
        else
            throw new IllegalArgumentException("Unknown compression name: " + name);
    }
}
