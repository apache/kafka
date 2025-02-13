package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KTableKTableLeftJoinBugFixTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Person> personsTopic;
  private TestInputTopic<String, Department> departmentsTopic;
  private TestOutputTopic<String, Person> outputTopic;

  @BeforeEach
  public void setup() {
    // Create the Kafka Streams application
    StreamsBuilder builder = new StreamsBuilder();

    // Define the departments KTable
    KTable<String, Department> departments = builder.table(
        "departments",
        Materialized.<String, Department>as(Stores.persistentKeyValueStore("departments"))
            .withKeySerde(Serdes.String())
            .withValueSerde(CustomJsonSerde.json(Department.class))
    );

    // Define the persons KTable
    KTable<String, Person> persons = builder.table(
        "persons",
        Materialized.<String, Person>as(Stores.persistentKeyValueStore("persons"))
            .withKeySerde(Serdes.String())
            .withValueSerde(CustomJsonSerde.json(Person.class))
    );

    KTable<String, Person> joined = persons.leftJoin(
        departments,
        Person::getDepartmentId,
        (person, department) -> {
          if (department == null) {
            return Person.builder()
                .id(person.getId()) // Ensure id is set
                .departmentId(person.getDepartmentId()) // Ensure departmentId is set
                .department(null)
                .build();
          } else {
            return Person.builder()
                .id(person.getId()) // Ensure id is set
                .departmentId(person.getDepartmentId()) // Ensure departmentId is set
                .department(department)
                .build();
          }
        },
        Materialized.<String, Person>as(Stores.persistentKeyValueStore("joined-results"))
            .withKeySerde(Serdes.String())
            .withValueSerde(CustomJsonSerde.json(Person.class))
    );

    // Write the join results to an output topic
    joined.toStream().to("joined-results", Produced.with(Serdes.String(), CustomJsonSerde.json(Person.class)));

    // Create the TopologyTestDriver
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-ktable-left-join-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomJsonSerde.json(Object.class).getClass().getName());

    testDriver = new TopologyTestDriver(builder.build(), props);

    // Create test topics
    personsTopic = testDriver.createInputTopic(
        "persons",
        Serdes.String().serializer(),
        CustomJsonSerde.json(Person.class).serializer()
    );

    departmentsTopic = testDriver.createInputTopic(
        "departments",
        Serdes.String().serializer(),
        CustomJsonSerde.json(Department.class).serializer()
    );

    outputTopic = testDriver.createOutputTopic(
        "joined-results",
        Serdes.String().deserializer(),
        CustomJsonSerde.json(Person.class).deserializer()
    );
  }

  @AfterEach
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void testLeftJoinIssue() {
    // Pre-populate departments
    departmentsTopic.pipeInput("dep-1", new Department("dep-1", "Department 1"));
    departmentsTopic.pipeInput("dep-2", new Department("dep-2", "Department 2"));

    // Create a person with FK = dep-1
    personsTopic.pipeInput("p-1", new Person("p-1", "dep-1"));

    // Verify the initial join result
    TestRecord<String, Person> result1 = outputTopic.readRecord();
    System.out.println("result1 : " + result1.value().toString() );
    assertEquals("dep-1", result1.value().getDepartment().getId()); // Join works

    // Create a person with FK = dep-2
    personsTopic.pipeInput("p-2", new Person("p-2", "dep-2"));

    // Verify the initial join result
    TestRecord<String, Person> result22 = outputTopic.readRecord();
    System.out.println("result2 : " + result22.value().toString() );
    assertEquals("dep-2", result22.value().getDepartment().getId()); // Join works



    // Update the person's FK to dep-2
    personsTopic.pipeInput("p-1", new Person("p-1", "dep-2"));

    // Verify the updated join result
    TestRecord<String, Person> result2 = outputTopic.readRecord();
    System.out.println("Update to 2 " + result2.value().toString());
    //assertEquals("dep-2", result2.value().getDepartment().getId()); // Join updates

    // Revert the person's FK back to dep-1
    personsTopic.pipeInput("p-1", new Person("p-1","dep-1"));

    // Verify the join result (this will fail without the fix)
    TestRecord<String, Person> result3 = outputTopic.readRecord();
    System.out.println("revert back " + result2.value().toString());
    //assertEquals("dep-1", result3.value().getDepartment().getId()); // Join fails without the fix
  }
}

class Person {
  private String id;
  private String departmentId;
  private Department department;

  // No-args constructor
  public Person() {
  }

  public Person( String id, String departmentId) {
    this.departmentId = departmentId;
    this.id = id;
  }

  // All-args constructor
  public Person(String id, String departmentId, Department department) {
    this.id = id;
    this.departmentId = departmentId;
    this.department = department;
  }

  // Builder pattern implementation
  public static PersonBuilder builder() {
    return new PersonBuilder();
  }

  // Getters and Setters
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getDepartmentId() {
    return departmentId;
  }

  public void setDepartmentId(String departmentId) {
    this.departmentId = departmentId;
  }

  public Department getDepartment() {
    return department;
  }

  public void setDepartment(Department department) {
    this.department = department;
  }

  // toString method
  @Override
  public String toString() {
    return "Person{" +
        "id='" + id + '\'' +
        ", departmentId='" + departmentId + '\'' +
        ", department=" + department +
        '}';
  }

  // equals and hashCode methods
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Person person = (Person) o;

    if (id != null ? !id.equals(person.id) : person.id != null) return false;
    if (departmentId != null ? !departmentId.equals(person.departmentId) : person.departmentId != null) return false;
    return department != null ? department.equals(person.department) : person.department == null;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (departmentId != null ? departmentId.hashCode() : 0);
    result = 31 * result + (department != null ? department.hashCode() : 0);
    return result;
  }

  // Builder class
  public static class PersonBuilder {
    private String id;
    private String departmentId;
    private Department department;

    PersonBuilder() {
    }

    public PersonBuilder id(String id) {
      this.id = id;
      return this;
    }

    public PersonBuilder departmentId(String departmentId) {
      this.departmentId = departmentId;
      return this;
    }

    public PersonBuilder department(Department department) {
      this.department = department;
      return this;
    }

    public Person build() {
      return new Person(id, departmentId, department);
    }

    @Override
    public String toString() {
      return "PersonBuilder{" +
          "id='" + id + '\'' +
          ", departmentId='" + departmentId + '\'' +
          ", department=" + department +
          '}';
    }
  }
}

class Department {
  private String id;
  private String name;

  public Department() {
  }

  // Constructor, getters, and setters
  public Department(String id, String name) {
    this.id = id;
    this.name = name;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "Department{" +
        "id='" + id + '\'' +
        ", name='" + name + '\'' +
        '}';
  }
}
