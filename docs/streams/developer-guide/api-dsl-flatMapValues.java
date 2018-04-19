// Split a sentence into words.
KStream<byte[], String> sentences = ...;
KStream<byte[], String> words = sentences.flatMapValues(value -> Arrays.asList(value.split("\\s+")));

// Java 7 example: cf. `mapValues` for how to create `ValueMapper` instances
