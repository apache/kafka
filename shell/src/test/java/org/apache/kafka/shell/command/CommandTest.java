public class CommandTest {
    @Test
    public void testParseCommands() {
        assertEquals(new CatCommandHandler(Arrays.asList("foo")),
                new Commands(true).parseCommand(Arrays.asList("cat", "foo")));
        assertEquals(new CdCommandHandler(Optional.empty()),
                new Commands(true).parseCommand(Arrays.asList("cd")));
        assertEquals(new CdCommandHandler(Optional.of("foo")),
                new Commands(true).parseCommand(Arrays.asList("cd", "foo")));
        assertEquals(new ExitCommandHandler(),
                new Commands(true).parseCommand(Arrays.asList("exit")));
        assertEquals(new HelpCommandHandler(),
                new Commands(true).parseCommand(Arrays.asList("help")));
        assertEquals(new HistoryCommandHandler(3),
                new Commands(true).parseCommand(Arrays.asList("history", "3")));
        assertEquals(new HistoryCommandHandler(Integer.MAX_VALUE),
                new Commands(true).parseCommand(Arrays.asList("history")));
        assertEquals(new LsCommandHandler(Collections.emptyList()),
                new Commands(true).parseCommand(Arrays.asList("ls")));
        assertEquals(new LsCommandHandler(Arrays.asList("abc", "123")),
                new Commands(true).parseCommand(Arrays.asList("ls", "abc", "123")));
        assertEquals(new PwdCommandHandler(),
                new Commands(true).parseCommand(Arrays.asList("pwd")));
    }

    @Test
    public void testParseInvalidCommand() {
        assertEquals(new ErroneousCommandHandler("invalid choice: 'blah' (choose " +
                        "from 'cat', 'cd', 'exit', 'find', 'help', 'history', 'ls', 'man', 'pwd', 'tree')"),
                new Commands(true).parseCommand(Arrays.asList("blah")));
    }

    @Test
    public void testEmptyCommandLine() {
        assertEquals(new NoOpCommandHandler(),
                new Commands(true).parseCommand(Arrays.asList("")));
        assertEquals(new NoOpCommandHandler(),
                new Commands(true).parseCommand(Collections.emptyList()));
    }
}