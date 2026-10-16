import java.util.Random;

public class MessageGenerator {
    private static final String[] TOPICS = {"cats", "dogs", "pizza", "code", "weather"};
    private static final String[] ACTIONS = {"loves", "hates", "eats", "debugs", "deploys"};
    private static final Random rand = new Random();
    
    private String generatorName;

    public MessageGenerator() {
        this.generatorName = "DefaultGenerator";
        System.out.println("Message Generator created!");
    }
    
    public MessageGenerator(String name) {
        this.generatorName = name;
        System.out.println("Message Generator '" + name + "' created!");
    }

    public String getGeneratorName() {
        return generatorName;
    }

    public void setGeneratorName(String newName) {
        this.generatorName = newName;
    }

    public String createMessage() {
        String topic = TOPICS[rand.nextInt(TOPICS.length)];
        String action = ACTIONS[rand.nextInt(ACTIONS.length)];
        return generatorName + " says: Someone " + action + " " + topic + "!";
    }

    public void showMessages(int count) {
        for (int i = 0; i < count; i++) {
            System.out.println(createMessage());
        }
    }

    public static void main(String[] args) {
        MessageGenerator gen = new MessageGenerator("FunBot");
        gen.showMessages(3);
        gen.setGeneratorName("SuperFunBot");
        System.out.println("\nName changed to: " + gen.getGeneratorName());
        gen.showMessages(2);
    }
}
