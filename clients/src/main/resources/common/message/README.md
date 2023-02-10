Apache Kafka Message Definitions
================================

Introduction
------------
The JSON files in this directory define the Apache Kafka message protocol.
This protocol describes what information clients and servers send to each
other, and how it is serialized.  Note that this version of JSON supports
comments.  Comments begin with a double forward slash.

When Kafka is compiled, these specification files are translated into Java code
to read and write messages.  Any change to these JSON files will trigger a
recompilation of this generated code.

These specification files replace an older system where hand-written
serialization code was used.  Over time, we will migrate all messages to using
automatically generated serialization and deserialization code.

Requests and Responses
----------------------
The Kafka protocol features requests and responses.  Requests are sent to a
server in order to get a response.  Each request is uniquely identified by a
16-bit integer called the "api key".  The API key of the response will always
match that of the request.

Each message has a unique 16-bit version number.  The schema might be different
for each version of the message.  Sometimes, the version is incremented even
though the schema has not changed.  This may indicate that the server should
behave differently in some way.  The version of a response must always match
the version of the corresponding request.

Each request or response has a top-level field named "validVersions."  This
specifies the versions of the protocol that our code understands.  For example,
specifying "0-2" indicates that we understand versions 0, 1, and 2.  You must
always specify the highest message version which is supported.

The only old message versions that are no longer supported are version 0 of
MetadataRequest and MetadataResponse.  In general, since we adopted KIP-97,
dropping support for old message versions is no longer allowed without a KIP.
Therefore, please be careful not to increase the lower end of the version
support interval for any message.

MessageData Objects
-------------------
Using the JSON files in this directory, we generate Java code for MessageData
objects.  These objects store request and response data for kafka.  MessageData
objects do not contain a version number.  Instead, a single MessageData object
represents every possible version of a Message.  This makes working with
messages more convenient, because the same code path can be used for every
version of a message.

Fields
------
Each message contains an array of fields.  Fields specify the data that should
be sent with the message.  In general, fields have a name, a type, and version
information associated with them.

The order that fields appear in a message is important.  Fields which come
first in the message definition will be sent first over the network.  Changing
the order of the fields in a message is an incompatible change.

In each new message version, we may add or subtract fields.  For example, if we
are creating a new version 3 of a message, we can add a new field with the
version spec "3+".  This specifies that the field only appears in version 3 and
later.  If a field is being removed, we should change its version from "0+" to
"0-2" to indicate that it will not appear in version 3 and later.

Field Types
-----------
There are several primitive field types available.

* "boolean": either true or false.

* "int8": an 8-bit integer.

* "int16": a 16-bit integer.

* "uint16": a 16-bit unsigned integer.

* "int32": a 32-bit integer.

* "uint32": a 32-bit unsigned integer.

* "int64": a 64-bit integer.

* "float64": is a double-precision floating point number (IEEE 754).

* "string": a UTF-8 string.

* "uuid": a type 4 immutable universally unique identifier.

* "bytes": binary data.

* "records": recordset such as memory recordset.

In addition to these primitive field types, there is also an array type.  Array
types start with a "[]" and end with the name of the element type.  For
example, []Foo declares an array of "Foo" objects.  Array fields have their own
array of fields, which specifies what is in the contained objects.

For information about how fields are serialized, see the [Kafka Protocol
Guide](https://kafka.apache.org/protocol.html).

Nullable Fields
---------------
Booleans, ints, and floats can never be null.  However, fields that are strings,
bytes, uuid, records, or arrays may optionally be "nullable".  When a field is 
"nullable", that simply means that we are prepared to serialize and deserialize
null entries for that field.

If you want to declare a field as nullable, you set "nullableVersions" for that
field.  Nullability is implemented as a version range in order to accommodate a
very common pattern in Kafka where a field that was originally not nullable
becomes nullable in a later version.

If a field is declared as non-nullable, and it is present in the message
version you are using, you should set it to a non-null value before serializing
the message.  Otherwise, you will get a runtime error.

Tagged Fields
-------------
Tagged fields are an extension to the Kafka protocol which allows optional data
to be attached to messages.  Tagged fields can appear at the root level of
messages, or within any structure in the message.

Unlike mandatory fields, tagged fields can be added to message versions that
already exists.  Older servers will ignore new tagged fields which they do not
understand.

In order to make a field tagged, set a "tag" for the field, and also set up
tagged versions for the field.  The taggedVersions you specify should be
open-ended-- that is, they should specify a start version, but not an end
version.

You can remove support for a tagged field from a specific version of a message,
but you can't reuse a tag once it has been used for something else.  Once tags
have been used for something, they can't be used for anything else, without
breaking compatibility.

Note that tagged fields can only be added to "flexible" message versions.

Flexible Versions
-----------------
Kafka serialization has been improved over time to be more flexible and
efficient.  Message versions that contain these improvements are referred to as
"flexible versions."

In flexible versions, variable-length fields such as strings, arrays, and bytes
fields are serialized in a more efficient way that saves space.  The new
serialization types start with compact.  For example COMPACT_STRING is a more
efficient form of STRING.

Serializing Messages
--------------------
The Message#write method writes out a message to a buffer.  The fields that are
written out will depend on the version number that you supply to write().  When
you write out a message using an older version, fields that are too old to be
present in the schema will be omitted.

When working with older message versions, please verify that the older message
schema includes all the data that needs to be sent.  For example, it is probably
OK to skip sending a timeout field.  However, a field which radically alters the
meaning of the request, such as a "validateOnly" boolean, should not be ignored.

It's often useful to know how much space a message will take up before writing
it out to a buffer.  You can find this out by calling the Message#size method.

Deserializing Messages
----------------------
Message objects may be deserialized using the Message#read method.  This method
overwrites all the data currently in the message object with new data.

Any fields in the message object that are not present in the version that you
are deserializing will be reset to default values.  Unless a custom default has
been set:

* Integer fields default to 0.

* Floats default to 0.

* Booleans default to false.

* Strings default to the empty string.

* Bytes fields default to the empty byte array.

* Uuid fields default to zero uuid.

* Records fields default to null.

* Array fields default to empty.

You can specify "null" as a default value for a string field by specifying the
literal string "null".  Note that you can only specify null as a default if all
versions of the field are nullable.

Custom Default Values
---------------------
You may set a custom default for fields that are integers, booleans, floats, or
strings.  Just add a "default" entry in the JSON object.  The custom default
overrides the normal default for the type.  So for example, you could make a
boolean field default to true rather than false, and so forth.

Note that the default must be valid for the field type.  So the default for an
int16 field must be an integer that fits in 16 bits, and so forth.  You may
specify hex or octal values, as long as they are prefixed with 0x or 0.  It is
currently not possible to specify a custom default for bytes or array fields.

Custom defaults are useful when an older message version lacked some
information.  For example, if an older request lacked a timeout field, you may
want to specify that the server should assume that the timeout for such a
request is 5000 ms (or some other arbitrary value).

Ignorable Fields
----------------
When we write messages using an older or newer format, not all fields may be
present.  The message receiver will fill in the default value for the field
during deserialization.  Therefore, if the source field was set to a non-default
value, that information will be lost.

In some cases, this information loss is acceptable.  For example, if a timeout
field does not get preserved, this is not a problem.  However, in other cases,
the field is really quite important and should not be discarded.  One example is
a "verify only" boolean which changes the whole meaning of the request.

By default, we assume that information loss is not acceptable.  The message
serialization code will throw an exception if the ignored field is not set to
the default value.  If information loss for a field is OK, please set
"ignorable" to true for the field to disable this behavior.  When ignorable is
set to true, the field may be silently omitted during serialization.

Hash Sets
---------
One very common pattern in Kafka is to load array elements from a message into
a Map or Set for easier access.  The message protocol makes this easier with
the "mapKey" concept.

If some of the elements of an array are annotated with "mapKey": true, the
entire array will be treated as a linked hash set rather than a list.  Elements
in this set will be accessible in O(1) time with an automatically generated
"find" function.  The order of elements in the set will still be preserved,
however.  New entries that are added to the set always show up as last in the
ordering.

Incompatible Changes
--------------------
It's very important to avoid making incompatible changes to the message
protocol.  Here are some examples of incompatible changes:

#### Making changes to a protocol version which has already been released.
Protocol versions that have been released must be regarded as done.  If there
were mistakes, they should be corrected in a new version rather than changing
the existing version.

#### Re-ordering existing fields.
It is OK to add new fields before or after existing fields.  However, existing
fields should not be re-ordered with respect to each other.

#### Changing the default of an existing field.
You must never change the default of a field which already exists.  Otherwise,
new clients and old servers will not agree on the default, and so forth.

#### Changing the type of an existing field.
One exception is that an array of primitives may be changed to an array of
structures containing the same data, as long as the conversion is done
correctly.  The Kafka protocol does not do any "boxing" of structures, so an
array of structs that contain a single int32 is the same as an array of int32s.
