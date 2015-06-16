from setuptools import find_packages, setup

setup(name="kafkatest",
      version="0.8.3-SNAPSHOT",
      description="Apache Kafka System Tests",
      author="Apache Kafka",
      platforms=["any"], 
      license="apache2.0",
      packages=find_packages(),
      requires=["ducktape(>=0.2.0)"]
      )
