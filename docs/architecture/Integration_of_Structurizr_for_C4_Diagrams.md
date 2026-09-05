# Integration of Structurizr for C4 Diagrams

This document provides guidance on using Structurizr to create C4 model architecture diagrams for the Apache Kafka project.

## Table of Contents

1. [Overview](#overview)
2. [C4 Model Introduction](#c4-model-introduction)
3. [Structurizr Overview](#structurizr-overview)
4. [Installation Options](#installation-options)
5. [DSL Language Reference](#dsl-language-reference)
6. [Kafka Architecture Example](#kafka-architecture-example)
7. [CI/CD Integration](#cicd-integration)
8. [Best Practices](#best-practices)
9. [References](#references)

---

## Overview

Structurizr is a "models as code" tool specifically designed to support the C4 model for visualizing software architecture. It uses a text-based Domain Specific Language (DSL) to define architecture models, which can then be rendered as interactive diagrams.

Key benefits:
- **Version Control**: Architecture diagrams stored as code in Git
- **Consistency**: Single model generates multiple views
- **Automation**: Integrate with CI/CD pipelines
- **Interactive**: Zoom, animate, and embed diagrams

---

## C4 Model Introduction

The C4 model, created by Simon Brown, provides a hierarchical approach to architecture visualization with four levels of abstraction:

### Level 1: System Context Diagram
Shows the software system in scope and its relationships with users and other systems. Answers: "What is the big picture?"

### Level 2: Container Diagram
Illustrates major structural building blocks (applications, services, databases) and technology choices. Answers: "What are the high-level technical components?"

### Level 3: Component Diagram
Breaks down containers into their constituent components and shows their interactions. Answers: "What are the internal building blocks?"

### Level 4: Code Diagram
Details classes, interfaces, and code-level elements. Typically auto-generated from code. Answers: "How is the component implemented?"

### Supplementary Diagrams
- **System Landscape**: Multiple systems and their relationships
- **Dynamic Diagrams**: Sequence/interaction flows
- **Deployment Diagrams**: Infrastructure and deployment topology

---

## Structurizr Overview

Structurizr provides several tools for working with C4 diagrams:

| Tool | Description | Use Case |
|------|-------------|----------|
| **Structurizr DSL** | Text-based language for defining models | Primary authoring format |
| **Structurizr Lite** | Local web-based viewer/editor | Development and preview |
| **Structurizr CLI** | Command-line utility | CI/CD integration, exports |
| **Structurizr Cloud** | Hosted service | Team collaboration |

---

## Installation Options

### Option 1: Structurizr Lite (Docker) - Recommended for Local Development

```bash
# Create a directory for your workspace
mkdir -p docs/architecture/structurizr
cd docs/architecture/structurizr

# Create a workspace.dsl file (see examples below)

# Run Structurizr Lite
docker pull structurizr/lite
docker run -it --rm -p 8080:8080 -v $(pwd):/usr/local/structurizr structurizr/lite
```

Access the diagrams at http://localhost:8080

### Option 2: Structurizr CLI

**Prerequisites**: Java 17+

```bash
# Download from GitHub releases
curl -L -o structurizr-cli.zip \
  https://github.com/structurizr/cli/releases/latest/download/structurizr-cli.zip
unzip structurizr-cli.zip -d structurizr-cli

# Validate a workspace
./structurizr-cli/structurizr.sh validate -workspace workspace.dsl

# Export to PlantUML
./structurizr-cli/structurizr.sh export -workspace workspace.dsl -format plantuml

# Export to Mermaid
./structurizr-cli/structurizr.sh export -workspace workspace.dsl -format mermaid
```

### Option 3: Structurizr CLI (Docker)

```bash
# Create an alias for convenience
alias structurizr="docker run --rm -v '${PWD}':/root/data -w /root/data structurizr/cli"

# Validate workspace
structurizr validate -workspace workspace.dsl

# Export diagrams
structurizr export -workspace workspace.dsl -format plantuml
```

---

## DSL Language Reference

### Basic Structure

```dsl
workspace "Workspace Name" "Description" {

    model {
        # Define people (actors)
        user = person "User" "Description of the user"

        # Define software systems
        system = softwareSystem "System Name" "Description" {
            # Define containers within the system
            container1 = container "Container Name" "Description" "Technology"
        }

        # Define relationships
        user -> system "Uses"
    }

    views {
        # System context view
        systemContext system "SystemContext" {
            include *
            autoLayout
        }

        # Container view
        container system "Containers" {
            include *
            autoLayout
        }

        # Styling
        styles {
            element "Software System" {
                background #1168bd
                color #ffffff
            }
            element "Container" {
                background #438dd5
                color #ffffff
            }
        }
    }
}
```

### Key DSL Keywords

| Keyword | Description |
|---------|-------------|
| `workspace` | Top-level container for model and views |
| `model` | Contains elements and relationships |
| `person` | Represents a user or actor |
| `softwareSystem` | A software system |
| `container` | A deployable unit (app, service, database) |
| `component` | A component within a container |
| `->` | Defines a relationship |
| `views` | Contains diagram definitions |
| `styles` | Visual styling for elements |
| `tags` | Categorization labels |
| `!include` | Include external DSL files |
| `!docs` | Attach Markdown documentation |
| `!adrs` | Include Architecture Decision Records |

---

## Kafka Architecture Example

Below is an example Structurizr DSL workspace for Apache Kafka:

```dsl
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

        # Deployment view
        deploymentEnvironment "Production" {
            deploymentNode "Kafka Cluster" {
                deploymentNode "Broker Node 1" "" "Linux VM" {
                    broker1 = containerInstance broker
                }
                deploymentNode "Broker Node 2" "" "Linux VM" {
                    broker2 = containerInstance broker
                }
                deploymentNode "Broker Node 3" "" "Linux VM" {
                    broker3 = containerInstance broker
                }
                deploymentNode "Controller Node" "" "Linux VM" {
                    controllerInstance = containerInstance controller
                }
            }
        }

        deployment kafka "Production" "KafkaDeployment" {
            include *
            autoLayout
            description "Deployment diagram for a 3-broker Kafka cluster"
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
```

Save this as `docs/architecture/structurizr/workspace.dsl` and run Structurizr Lite to view.

---

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Generate Architecture Diagrams

on:
  push:
    paths:
      - 'docs/architecture/structurizr/**'
  pull_request:
    paths:
      - 'docs/architecture/structurizr/**'

jobs:
  validate-and-export:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Validate Structurizr workspace
        run: |
          docker run --rm \
            -v ${{ github.workspace }}/docs/architecture/structurizr:/root/data \
            -w /root/data \
            structurizr/cli validate -workspace workspace.dsl

      - name: Export to PNG
        run: |
          docker run --rm \
            -v ${{ github.workspace }}/docs/architecture/structurizr:/root/data \
            -w /root/data \
            structurizr/cli export -workspace workspace.dsl -format png

      - name: Export to PlantUML
        run: |
          docker run --rm \
            -v ${{ github.workspace }}/docs/architecture/structurizr:/root/data \
            -w /root/data \
            structurizr/cli export -workspace workspace.dsl -format plantuml

      - name: Upload diagrams
        uses: actions/upload-artifact@v4
        with:
          name: architecture-diagrams
          path: docs/architecture/structurizr/*.png
```

### Gradle Integration

```kotlin
// build.gradle.kts
tasks.register<Exec>("validateArchitecture") {
    group = "documentation"
    description = "Validate Structurizr workspace"
    commandLine(
        "docker", "run", "--rm",
        "-v", "${projectDir}/docs/architecture/structurizr:/root/data",
        "-w", "/root/data",
        "structurizr/cli", "validate", "-workspace", "workspace.dsl"
    )
}

tasks.register<Exec>("exportArchitectureDiagrams") {
    group = "documentation"
    description = "Export architecture diagrams to PNG"
    dependsOn("validateArchitecture")
    commandLine(
        "docker", "run", "--rm",
        "-v", "${projectDir}/docs/architecture/structurizr:/root/data",
        "-w", "/root/data",
        "structurizr/cli", "export", "-workspace", "workspace.dsl", "-format", "png"
    )
}
```

---

## Best Practices

### 1. Organize DSL Files
Use `!include` to split large workspaces:

```
docs/architecture/structurizr/
├── workspace.dsl          # Main workspace file
├── model/
│   ├── people.dsl         # Actor definitions
│   ├── systems.dsl        # External systems
│   └── kafka.dsl          # Kafka containers/components
├── views/
│   ├── context.dsl        # Context views
│   ├── containers.dsl     # Container views
│   └── styles.dsl         # Shared styles
└── docs/
    └── decisions/         # Architecture Decision Records
```

### 2. Use Tags for Styling
Apply tags to categorize elements and apply consistent styling:

```dsl
broker = container "Kafka Broker" "..." "Scala" {
    tags "Critical" "Core"
}
```

### 3. Include Documentation
Embed documentation directly in the workspace:

```dsl
workspace {
    !docs docs/
    !adrs docs/decisions/

    model { ... }
    views { ... }
}
```

### 4. Version Control Best Practices
- Commit `.dsl` files alongside code changes
- Review architecture changes in PRs
- Use branch protection for architecture files
- Generate and commit PNG exports for documentation

### 5. Keep Diagrams Focused
- Level 1 (Context): Maximum 10-15 elements
- Level 2 (Container): Maximum 15-20 elements
- Level 3 (Component): Maximum 20-25 elements
- Use filtered views for complex systems

---

## References

### Official Documentation
- [Structurizr Documentation](https://docs.structurizr.com/)
- [Structurizr DSL Reference](https://docs.structurizr.com/dsl)
- [Structurizr DSL Language Reference](https://docs.structurizr.com/dsl/language)
- [C4 Model](https://c4model.com/)

### Tools
- [Structurizr Lite Docker Image](https://hub.docker.com/r/structurizr/lite)
- [Structurizr CLI Docker Image](https://hub.docker.com/r/structurizr/cli)
- [Structurizr CLI GitHub](https://github.com/structurizr/cli)

### Tutorials and Guides
- [Structurizr DSL Tutorial](https://docs.structurizr.com/dsl/tutorial)
- [Structurizr Lite Quickstart](https://docs.structurizr.com/lite/quickstart)
- [Getting Started with Structurizr Lite](https://dev.to/simonbrown/getting-started-with-structurizr-lite-27d0)

### Related Topics
- [Mapping Complex Distributed Systems with C4 Diagrams](https://blog.glen-thomas.com/architecture/2025/08/27/mapping-complex-distributed-systems-with-c4-diagrams-and-structurizr-dsl.html)
- [Apache Kafka Architecture](https://kafka.apache.org/documentation/#design)
