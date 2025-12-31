# Notes on ProduceResponse → ProducerBatch matching

While reading `Sender.handleProduceResponse`, I wanted to write down a few questions
before making any code changes.

The response-to-batch lookup currently relies on:
- topicId when present (newer protocol versions)
- falling back to topic name + partition otherwise
- a snapshot of topicId → name mapping taken before send

Given that metadata can change while a produce request is in flight (topic recreation,
leader changes, protocol differences), I wanted to confirm the guarantees here.

In particular:
- Are we assuming that a response with a non-zero topicId can always be resolved
  using the topic-name snapshot taken before sending?
- In what cases (if any) can `batch == null` occur, and is that considered an
  impossible state or a deliberate hard failure?

This is just a note for now — no behavior change proposed yet.
