## Helm Chart for deploying Kafka to Kubernetes

This Helm chart deploys Kafka configured with KRaft to a Kubernetes cluster. It currently only supports separate pods for the controllers and brokers.

The number of broker and controller pods is set using the values
```Yaml
broker:
  replicas: 3
controller:
  replicas: 3
```

The brokers and controllers are deployed as Kubernetes [StatefulSets][statefulset].

The node ids are set to `broker.baseId`/`controller.baseId` plus the instance id of the StatefuleSet pod. The baseIds default to 1 and 11, respectively.

Using StatefuleSets ensures each pod has it's own dedicated storage. The type of storage will depend on the configuration of the Kubernetes cluster. By default, the default storage class will be used. To use a specific storage class override the value `storageClassName`. To specific the requested size for the storage set `logDirCapacity`, for example:
```Yaml
storageClassName: local-path
logDirCapacity: 2Gi
```

Access to brokers and controllers is done using headless services so that workloads accessing Kafka can connect to the brokers using the Kubernetes internal DNS. A broker URL would therefore look like

`kafka-0.brokers.kafka.svc.cluster.local:9092`

### Setting Properties

This helm chart work by templating the properties files into a [ConfigMap][configmap]. These are mounted into the running containers at the location `/mnt/config/`. The new launch script [`k8s_launch`](../docker/jvm/k8s_launch) copies the appropriate files to the location where Kafka needs them to run.

Kafka properties are defined using a dot (`.`) as a name delimiter, e.g. `this.is.a.property.name`. Values in Helm are managed as Yaml and when setting values on the command line, the dot notation is used to specify nested values. Using `--set this.is.a.property.name=value` would therefore be defined as a nested Yaml value of

```Yaml
this:
  is:
    a:
      property:
        name: 'value'
```

This would be a complex (and somewhat strange) way to define Kafka properties so this Helm chart handles properties set with the dots as they would appear in the properties file.

Properties can be set in a Yaml values file as:
```Yaml
properties:
  network.threads: 3
  num.io.threads: 8
  partitions: 32
```
but can also be specified on the command line for example `--set properties.num.io.threads=8`.

There are three sections in the value file for properties:
```Yaml
properties:
  network.threads: 3
  ...

broker:
  properties:
    ...

controller:
  properties:
    ...
```

Values in `properties` are applied to both controllers and brokers. Values in `broker.properties` apply to only the brokers, and are merged with the common properties with the broker specific value taking precedent, to prevent duplicate values. Likewise for the controllers.

The following properties are calculated based on the deployment settings and cannot be overridden:
  * `process.roles`
  * `node.id`
  * `controller.quorum.voters`
  * `listeners`
  * `controller.listener.names`
  * `log.dirs`

## Logging Configuration

Now that Kafka uses log4j2, the configuration for logging matches that in the log4j2.yaml files.

The two logging sections are:

```Yaml
logging:
  Properties: ...
  Appenders: ...
  Loggers: ...

toolLogging:
  Properties: ...
  Appenders: ...
  Loggers: ...
```

So, for example, to override the console log pattern you could specify:

`--set logging.Appenders.Console.PatternLayout.pattern="[%d] %p %m (%c)%n"`

[configmap]: https://kubernetes.io/docs/concepts/configuration/configmap/ "A ConfigMap is an API object used to store non-confidential data in key-value pairs"
[statefulset]: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/ "A StatefulSet runs a group of Pods, and maintains a sticky identity for each of those Pods"