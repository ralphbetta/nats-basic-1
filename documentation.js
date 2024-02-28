

/*

Stream:

A stream is a durable, append-only log of messages. It represents a sequence of data records ordered by time.
Streams are often used to store and manage data streams, such as event logs, telemetry data, or any other kind of sequentially ordered data.
Streams in NATS JetStream are defined by a name and can have configuration settings like the maximum number of messages, message retention policy, or the subjects they subscribe to.


Consumer:

A consumer is a client application or component that reads messages from a stream.
Consumers subscribe to a stream and receive messages that are published to the corresponding subjects.
Consumers can have different configurations like the acknowledgment policy, delivery subject, or maximum delivery attempts.




Message:

A message is a unit of data sent from a publisher to a stream and then consumed by one or more consumers.
Messages typically contain payload data along with optional metadata like timestamps, sequence numbers, or other headers.
Messages are stored in the stream and delivered to consumers based on their subscription.
*/