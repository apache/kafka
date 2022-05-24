./gradlew jar
./gradlew srcJar
./gradlew aggregatedJavadoc
./gradlew javadoc
./gradlew javadocJar # builds a javadoc jar for each module
./gradlew scaladoc
./gradlew scaladocJar # builds a scaladoc jar for each module
./gradlew docsJar # builds both (if applicable) javadoc and scaladoc jars for each modul
./gradlew clean
./gradlew releaseTarG

cd core/build/distributions/
tar -xzf kafka_2.13-3.1.1-SNAPSHOT.tgz
cd ../../../