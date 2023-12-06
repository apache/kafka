Kafka Docker Image Examples
---------------------------

- This directory contains docker compose files for some example configs to run docker image.

- Run the commands from root of the repository.

- To bring up the docker compose examples, use docker compose command.

For example:-
```
# This command brings up JVM cluster
$ docker compose -f docker/examples/jvm/cluster/docker-compose.yml up
```

- Kafka server can be accessed using cli scripts or your own client code.
Make sure jars are built for the code, if cli scripts are being used.

For example:-
```
# Produce messages to kafka broker
$ bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:29092
```

- Use `./docker/examples/fixtures/client-secrets` for connecting with Kafka when running SSL example. `./docker/examples/fixtures/client-secrets/client-ssl.properties` file can be used as client config.

For example:-
```
# Produce message to SSL Kafka example
$ bin/kafka-console-producer.sh --topic test_topic_ssl --bootstrap-server localhost:9093 --producer.config ./docker/examples/fixtures/client-secrets/client-ssl.properties
```
