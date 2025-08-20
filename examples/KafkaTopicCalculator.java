public class KafkaTopicCalculator {

    public int totalParts(int partPerTopic, int topicCount) {
        return partPerTopic * topicCount;
    }

    public double storageGB(int msgSize, int msgsPerDay, int days) {
        double dailyBytes = msgSize * msgsPerDay;
        double totalBytes = dailyBytes * days;
        return totalBytes / (1024.0 * 1024.0 * 1024.0);
    }

    public static void main(String[] args) {
        KafkaTopicCalculator calc = new KafkaTopicCalculator();
        System.out.println("Total Partitions: " + calc.totalParts(6, 10));
        System.out.println("Estimated Storage: " + calc.storageGB(1024, 1000000, 7) + " GB");
    }
}
