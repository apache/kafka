.. _uses:

Use Cases
---------

.. contents::
    :local:

Here is a description of a few of the popular use cases for Apache
Kafka®. For an overview of a number of these areas in action, see `this
blog
post <https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying/>`__.

.. _uses_messaging:

Messaging
=========

Kafka works well as a replacement for a more traditional message broker.
Message brokers are used for a variety of reasons (to decouple
processing from data producers, to buffer unprocessed messages, etc). In
comparison to most messaging systems Kafka has better throughput,
built-in partitioning, replication, and fault-tolerance which makes it a
good solution for large scale message processing applications.

In our experience messaging uses are often comparatively low-throughput,
but may require low end-to-end latency and often depend on the strong
durability guarantees Kafka provides.

In this domain Kafka is comparable to traditional messaging systems such
as `ActiveMQ <http://activemq.apache.org>`__ or
`RabbitMQ <https://www.rabbitmq.com>`__.

.. _uses_website:

Website Activity Tracking
=========================

The original use case for Kafka was to be able to rebuild a user
activity tracking pipeline as a set of real-time publish-subscribe
feeds. This means site activity (page views, searches, or other actions
users may take) is published to central topics with one topic per
activity type. These feeds are available for subscription for a range of
use cases including real-time processing, real-time monitoring, and
loading into Hadoop or offline data warehousing systems for offline
processing and reporting.

Activity tracking is often very high volume as many activity messages
are generated for each user page view.

.. _uses_metrics:

Metrics
=======

Kafka is often used for operational monitoring data. This involves
aggregating statistics from distributed applications to produce
centralized feeds of operational data.

.. _uses_logs:

Log Aggregation
===============

Many people use Kafka as a replacement for a log aggregation solution.
Log aggregation typically collects physical log files off servers and
puts them in a central place (a file server or HDFS perhaps) for
processing. Kafka abstracts away the details of files and gives a
cleaner abstraction of log or event data as a stream of messages. This
allows for lower-latency processing and easier support for multiple data
sources and distributed data consumption. In comparison to log-centric
systems like Scribe or Flume, Kafka offers equally good performance,
stronger durability guarantees due to replication, and much lower
end-to-end latency.

.. _uses_streamprocessing:

Stream Processing
=================

Many users of Kafka process data in processing pipelines consisting of
multiple stages, where raw input data is consumed from Kafka topics and
then aggregated, enriched, or otherwise transformed into new topics for
further consumption or follow-up processing. For example, a processing
pipeline for recommending news articles might crawl article content from
RSS feeds and publish it to an "articles" topic; further processing
might normalize or deduplicate this content and published the cleansed
article content to a new topic; a final processing stage might attempt
to recommend this content to users. Such processing pipelines create
graphs of real-time data flows based on the individual topics. Starting
in 0.10.0.0, a light-weight but powerful stream processing library
called `Kafka Streams </documentation/streams>`__ is available in Apache
Kafka to perform such data processing as described above. Apart from
Kafka Streams, alternative open source stream processing tools include
`Apache Storm <https://storm.apache.org/>`__ and `Apache
Samza <http://samza.apache.org/>`__.

.. _uses_eventsourcing:

Event Sourcing
==============

`Event sourcing <http://martinfowler.com/eaaDev/EventSourcing.html>`__
is a style of application design where state changes are logged as a
time-ordered sequence of records. Kafka's support for very large stored
log data makes it an excellent backend for an application built in this
style.

.. _uses_commitlog:

Commit Log
==========

Kafka can serve as a kind of external commit-log for a distributed
system. The log helps replicate data between nodes and acts as a
re-syncing mechanism for failed nodes to restore their data. The `log
compaction </documentation.html#compaction>`__ feature in Kafka helps
support this usage. In this usage Kafka is similar to `Apache
BookKeeper <http://zookeeper.apache.org/bookkeeper/>`__ project.
