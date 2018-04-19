.. _streams_developer-guide_topics:

`Managing Streams Application Topics <#managing-streams-application-topics>`__
==============================================================================

A Kafka Streams application continuously reads from Kafka topics,
processes the read data, and then writes the processing results back
into Kafka topics. The application may also auto-create other Kafka
topics in the Kafka brokers, for example state store changelogs topics.
This section describes the differences these topic types and how to
manage the topics and your applications.

.. contents:: Table of Contents
   :local:

Kafka Streams distinguishes between `user
topics <#streams-developer-guide-topics-user>`__ and `internal
topics <#streams-developer-guide-topics-internal>`__.

`User topics <#user-topics>`__
------------------------------

User topics exist externally to an application and are read from or
written to by the application, including:

Input topics
    Topics that are specified via source processors in the application’s
    topology; e.g. via ``StreamsBuilder#stream()``,
    ``StreamsBuilder#table()`` and ``Topology#addSource()``.
Output topics
    Topics that are specified via sink processors in the application’s
    topology; e.g. via ``KStream#to()``, ``KTable.to()`` and
    ``Topology#addSink()``.
Intermediate topics
    Topics that are both input and output topics of the application’s
    topology; e.g. via ``KStream#through()``.

User topics must be created and manually managed ahead of time (e.g.,
via the `topic
tools <../../kafka/post-deployment.html#kafka-operations-admin>`__). If
user topics are shared among multiple applications for reading and
writing, the application users must coordinate topic management. If user
topics are centrally managed, then application users then would not need
to manage topics themselves but simply obtain access to them.

.. note::

   You should not use the auto-create topic feature on the brokers to
   create user topics, because:

   -  Auto-creation of topics may be disabled in your Kafka cluster.
   -  Auto-creation automatically applies the default topic settings such
      as the replicaton factor. These default settings might not be what
      you want for certain output topics (e.g.,
      ``auto.create.topics.enable=true`` in the `Kafka broker
      configuration <http://kafka.apache.org/0100/documentation.html#brokerconfigs>`__).

`Internal topics <#internal-topics>`__
--------------------------------------

Internal topics are used internally by the Kafka Streams application
while executing, for example the changelog topics for state stores.
These topics are created by the application and are only used by that
stream application.

If security is enabled on the Kafka brokers, you must grant the
underlying clients admin permissions so that they can create internal
topics set. For more information, see `Streams
Security <security.html#streams-developer-guide-security>`__.

.. note::

   The internal topics follow the naming convention
   ``<application.id>-<operatorName>-<suffix>``, but this convention is not
   guaranteed for future releases.

