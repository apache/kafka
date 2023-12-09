Kafka Docker Image Examples
---------------------------

- This directory contains docker compose files for some example configs to run docker image.

- Run the commands from root of the repository.

- Checkout `single-node` examples for quick small examples to play around with.

- `cluster` contains multi node examples, for `combined` mode as well as `isolated` mode.

- To bring up the docker compose examples, use docker compose command:-
```
# This command brings up JVM cluster in combined mode
$ docker compose -f docker/examples/jvm/cluster/combined/plaintext/docker-compose.yml up
```

- Kafka server can be accessed using cli scripts or your own client code.
Make sure jars are built, if you decide to use cli scripts of this repo.

For example:-
```
# Produce messages to kafka broker
$ bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:29092
```

- Use `./docker/examples/fixtures/client-secrets` for connecting with Kafka when running SSL examples. `./docker/examples/fixtures/client-secrets/client-ssl.properties` file can be used as client config.

- Start the single node ssl example:-
```
# This command brings up single node kafka server which can be accessed using
$ docker compose -f docker/examples/jvm/single-node/ssl/docker-compose.yml up
```
- To access ssl kafka server following command can be used:-
```
# Produce message to single node SSL Kafka example
$ bin/kafka-console-producer.sh --topic test_topic_ssl --bootstrap-server localhost:9093 --producer.config ./docker/examples/fixtures/client-secrets/client-ssl.properties
```

- Note that - the examples are meant to be tried one at a time, make sure you close an example server before trying out the other to avoid conflicts.
