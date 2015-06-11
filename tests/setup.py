from setuptools import find_packages, setup

setup(name="kafkatest",
      version="0.1",
      description="System tests for Apache Kafka",
      author="Ewen Cheslack-Postava <ewen@confluent.io>, Geoff Anderson <geoff@confluent.io>",
      platforms=["any"], 
      license="apache2.0",
      packages=find_packages(),
      )
