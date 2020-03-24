package kafka.log;

public enum SegmentStatus {
    HOT(1), DELETED(2), CLEANED(3), SWAP(4), UNKNOWN(5);

    private final int status;
    SegmentStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static SegmentStatus getStatus(int id){
        switch (id){
            case 1:
                return HOT;
            case 2:
                return DELETED;
            case 3:
                return CLEANED;
            case 4:
                return SWAP;
            case 5:
                return UNKNOWN;
            default:
                throw new RuntimeException("Invalid file status : "+id);
        }
    }
}
