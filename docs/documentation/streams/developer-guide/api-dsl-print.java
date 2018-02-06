KStream<byte[], String> stream = ...;
// print to sysout
stream.print();

// print to file with a custom label
stream.print(Printed.toFile("streams.out").withLabel("streams"));
