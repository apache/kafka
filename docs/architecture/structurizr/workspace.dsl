workspace "Apache Kafka" "C4 Model for Apache Kafka Architecture" {

    model {
        # External actors
        producer = person "Producer Application" "Application that publishes messages to Kafka topics"
        consumer = person "Consumer Application" "Application that subscribes to and processes messages from Kafka topics"
        admin = person "Kafka Admin" "Administrator who manages Kafka cluster configuration"

        # Kafka Software System
        kafka = softwareSystem "Apache Kafka" "Distributed event streaming platform" {

            # Core containers
            broker = container "Kafka Broker" "Handles message storage and serving" "Scala/Java" "Broker"
            controller = container "KRaft Controller" "Manages cluster metadata and leader election" "Scala/Java" "Controller"

            # Storage
            logSegment = container "Log Segments" "Append-only log files for message storage" "File System" "Storage"

            # Client libraries
            producerApi = container "Producer API" "Client library for publishing messages" "Java" "API"
            consumerApi = container "Consumer API" "Client library for consuming messages" "Java" "API"
            adminApi = container "Admin API" "Client library for cluster administration" "Java" "API"

            # Kafka Connect
            connect = container "Kafka Connect" "Framework for connecting external systems" "Java" "Connect"

            # Kafka Streams
            streams = container "Kafka Streams" "Stream processing library" "Java" "Streams"
        }

        # External systems
        externalDb = softwareSystem "External Database" "Source or sink for Kafka Connect" "External"
        monitoringSystem = softwareSystem "Monitoring System" "Prometheus, Grafana for metrics" "External"

        # Relationships
        producer -> producerApi "Uses"
        producerApi -> broker "Publishes messages to" "TCP/9092"

        consumer -> consumerApi "Uses"
        consumerApi -> broker "Consumes messages from" "TCP/9092"

        admin -> adminApi "Uses"
        adminApi -> broker "Manages" "TCP/9092"

        broker -> logSegment "Persists messages to"
        broker -> controller "Reports metadata to" "TCP/9093"
        controller -> broker "Manages leadership for"

        connect -> broker "Reads/writes data"
        connect -> externalDb "Syncs data with"

        streams -> broker "Processes streams from"

        broker -> monitoringSystem "Exposes metrics to" "JMX/HTTP"
    }

    views {
        # System Context
        systemContext kafka "KafkaContext" {
            include *
            autoLayout
            description "System context diagram for Apache Kafka"
        }

        # Container View
        container kafka "KafkaContainers" {
            include *
            autoLayout
            description "Container diagram showing Kafka internal components"
        }

        # Dynamic view: Message flow
        dynamic kafka "MessageFlow" "Shows the flow of a message from producer to consumer" {
            producer -> producerApi "1. Creates message"
            producerApi -> broker "2. Sends to partition leader"
            broker -> logSegment "3. Appends to log"
            broker -> consumerApi "4. Consumer fetches"
            consumerApi -> consumer "5. Delivers message"
            autoLayout
        }

        styles {
            element "Software System" {
                background #1168bd
                color #ffffff
                shape RoundedBox
            }
            element "External" {
                background #999999
                color #ffffff
            }
            element "Container" {
                background #438dd5
                color #ffffff
            }
            element "Broker" {
                background #ff6600
                color #ffffff
                shape Hexagon
            }
            element "Controller" {
                background #00cc66
                color #ffffff
                shape Hexagon
            }
            element "Storage" {
                background #666666
                color #ffffff
                shape Cylinder
            }
            element "API" {
                background #85bbf0
                color #000000
            }
            element "Connect" {
                background #9933ff
                color #ffffff
            }
            element "Streams" {
                background #ff3399
                color #ffffff
            }
            element "Person" {
                background #08427b
                color #ffffff
                shape Person
            }
            relationship "Relationship" {
                thickness 2
            }
        }
    }
}
