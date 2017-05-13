#!/bin/sh

install_dir=/work/install/kafka

./gradlew clean
./gradlew releaseTarGz -x signArchives

rm -r $install_dir/*
cp core/build/distributions/kafka_2.10-0.11.0.0-SNAPSHOT.tgz $install_dir
cd $install_dir
tar -xzvf kafka_2.10-0.11.0.0-SNAPSHOT.tgz
ln -s kafka_2.10-0.11.0.0-SNAPSHOT latest
cd -

