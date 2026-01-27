---
title: Kafka Streams Groups Tool
type: docs
description: 
weight: 14
tags: ['kafka', 'docs']
aliases: 
keywords: 
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

Use `kafka-streams-groups.sh` to manage **Streams groups** for the Streams Rebalance Protocol (KIP‑1071): list and describe groups, inspect members and offsets/lag, reset or delete offsets for input topics, and delete groups (optionally including internal topics).


# Overview

A **Streams group** is a broker‑coordinated group type for Kafka Streams that uses Streams‑specific RPCs and metadata, distinct from classic consumer groups. The CLI surfaces Streams‑specific states, assignments, and input‑topic offsets to simplify visibility and administration.

**Use with care:** Mutating operations (offset resets/deletes, group deletion) affect how applications will reprocess data when restarted. Always preview with \--dry-run before executing and ensure application instances are stopped/inactive and the group is empty before executing the command. 

# What the Streams Groups tool does

  * **List Streams groups** across a cluster and display or filter by group state (Empty, Not Ready, Assigning, Reconciling, Stable, Dead).
  * **Describe a Streams group** and show: 
    * Group state, group epoch, target assignment epoch (with `--state`, `--verbose` for additional details).
    * Per‑member info such as epochs, current vs target assignments, and whether a member still uses the classic protocol (with `--members` and `--verbose`).
    * Input‑topic offsets and lag (with `--offsets`), to understand how far behind processing is.
  * **Reset input‑topic offsets** for a Streams group to control reprocessing boundaries using precise specifiers (earliest, latest, to‑offset, to‑datetime, by‑duration, shift‑by, from‑file). Requires `--dry-run` or `--execute` and inactive instances.
  * **Delete offsets** for input topics to force re‑consumption on next start.
  * **Delete a Streams group** to clean up broker‑side Streams metadata (offsets, topology, assignments). Optionally delete all, or a subset of, **internal topics** at the same time using `--internal-topics`.



# Usage

The script is located in `bin/kafka-streams-groups.sh` and connects to your cluster via `--bootstrap-server`. For secured clusters, pass AdminClient properties using `--command-config`.
    
    
    $ kafka-streams-groups.sh --bootstrap-server <host:port> [COMMAND] [OPTIONS]

**Note:** `kafka-streams-groups.sh` complements the Streams Admin API for Streams groups. The CLI exposes list/describe/delete operations and offset management similar in spirit to consumer-group tools, but tailored to Streams groups defined in KIP‑1071. 

# Commands

## List Streams groups

Discovering groups
    
    
    # List all Streams groups
    kafka-streams-groups.sh --bootstrap-server localhost:9092 --list
    

## Describe Streams groups

Inspecting group's state, members, and lag
    
    
    # Describe a group: state + epochs
    kafka-streams-groups.sh --bootstrap-server localhost:9092 \
      --describe --group my-streams-app --state --verbose
    
    # Describe a group: members (assignments vs target, classic/streams)
    kafka-streams-groups.sh --bootstrap-server localhost:9092 \
      --describe --group my-streams-app --members --verbose
    
    # Describe a group: input-topic offsets and lag
    kafka-streams-groups.sh --bootstrap-server localhost:9092 \
      --describe --group my-streams-app --offsets
    

## Reset input-topic offsets (preview, then apply) {#reset-offsets}

Ensure all application instances are stopped/inactive. Always preview changes with `--dry-run` before using `--execute`.
    
    
    # Preview resetting all input topics to a specific timestamp
    kafka-streams-groups.sh --bootstrap-server localhost:9092 \
      --group my-streams-app \
      --reset-offsets --all-input-topics --to-datetime 2025-01-31T23:57:00.000 \
      --dry-run
    
    # Apply the reset
    kafka-streams-groups.sh --bootstrap-server localhost:9092 \
      --group my-streams-app \
      --reset-offsets --all-input-topics --to-datetime 2025-01-31T23:57:00.000 \
      --execute
    

## Delete offsets to force re-consumption

Delete offsets for all or specific input topics to have the group re-read data on restart.
    
    
    # Delete offsets for all input topics (execute)
    kafka-streams-groups.sh --bootstrap-server localhost:9092 \
      --group my-streams-app \
      --delete-offsets --all-input-topics --execute
    
    # Delete offsets for specific topics
    kafka-streams-groups.sh --bootstrap-server localhost:9092 \
      --group my-streams-app \
      --delete-offsets --topic input-a --topic input-b --execute
    

## Delete a Streams group (cleanup)

Delete broker-side Streams metadata for a group and optionally remove a subset of internal topics.
    
    
    # Delete Streams group metadata
    kafka-streams-groups.sh --bootstrap-server localhost:9092 \
      --delete --group my-streams-app
    
    # Delete a subset of internal topics alongside the group (use with care)
    kafka-streams-groups.sh --bootstrap-server localhost:9092 \
      --delete --group my-streams-app \
      --internal-topics my-app-repartition-0,my-app-changelog
    

# All options and flags

## Core actions

  * `--list`: List Streams groups. Use `--state` to display/filter by state.
  * `--describe`: Describe a group selected by `--group`. Combine with: 
    * `--state` (group state and epochs), `--members` (members and assignments), `--offsets` (input and repartition topics offsets/lag).
    * `--verbose` for additional details (e.g., leader epochs where applicable).
  * `--reset-offsets`: Reset input-topic offsets (one group at a time; instances should be inactive). Choose exactly one specifier: 
    * `--to-earliest`, `--to-latest`, `--to-current`, `--to-offset <n>`
    * `--by-duration <PnDTnHnMnS>`, `--to-datetime <YYYY-MM-DDTHH:mm:SS.sss>`
    * `--shift-by <n>` (±), `--from-file` (CSV)
Scope: 
    * `--all-input-topics` or one/more `--topic <name>`; some builds also support `--all-topics` (all input topics per broker topology metadata).
Safety: 
    * Requires `--dry-run` or `--execute`.
  * `--delete-offsets`: Delete offsets for `--all-input-topics`, specific `--topic` names, or `--from-file`.
  * `--delete`: Delete Streams group metadata; optionally pass `--internal-topics <list>` to delete a subset of internal topics.



## Common flags

  * `--group <id>`: Target Streams group (application.id).
  * `--all-groups`: Operate on all groups (allowed with `--delete`).
  * `--bootstrap-server <host:port>`: Broker(s) to connect to (required).
  * `--command-config <file>`: Properties for AdminClient (security, timeouts, etc.).
  * `--timeout <ms>`: Wait time for group stabilization in some operations (default: 5000ms).
  * `--dry-run`, `--execute`: Preview vs apply for mutating operations.
  * `--help`, `--version`, `--verbose`: Usage, version, verbosity.



# Best practices and safety

  * Preview changes with `--dry-run` to verify topic scope and impact before `--execute`.
  * Use `--internal-topics` carefully: deleting internal topics removes state backing topics; only do this when you intend to rebuild state from input topics.



This page documents `kafka-streams-groups.sh` capabilities for Streams groups as defined by KIP‑1071 and implemented in Apache Kafka.

  * [Documentation](/documentation)
  * [Kafka Streams](/documentation/streams)
  * [Developer Guide](/documentation/streams/developer-guide/)


