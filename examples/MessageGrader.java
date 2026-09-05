public class MessageGrader {
    public String gradeSpeed(int messagesPerSecond) {
        if (messagesPerSecond > 10000) return "A+";
        else if (messagesPerSecond > 5000) return "B";
        else if (messagesPerSecond > 1000) return "C";
        else return "F";
    }
    
    public static void main(String[] args) {
        MessageGrader grader = new MessageGrader();
        System.out.println("Grade: " + grader.gradeSpeed(7500));
    }
}
