Docker Images
=============

This directory contains scripts to build, test, push and promote docker image for kafka.

Local Setup
-----------
Make sure you have python (>= 3.7.x) and java (>= 17) (java needed only for running tests) installed before running the tests and scripts.

Run `pip install -r requirements.txt` to get all the requirements for running the scripts.

Make sure you have docker installed with support for buildx enabled. (For pushing multi-architecture image to docker registry)

Bulding image and running tests locally
---------------------------------------
- `docker_build_test.py` script builds and tests the docker image.
- kafka binary tarball url along with image name, tag and type is needed to build the image. For detailed usage description check `python docker_build_test.py --help`.
- Sanity tests for the docker image are present in test/docker_sanity_test.py.
- By default image will be built and tested, but if you only want to build the image, pass `--build` (or `-b`) flag and if you only want to test the given image pass `--test` (or `-t`) flag.
- An html test report will be generated after the tests are executed containing the results.

Example command:-
To build and test an image named test under kafka namespace with 3.6.0 tag and jvm image type ensuring kafka to be containerised should be https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz (it is recommended to use scala 2.13 binary tarball), following command can be used
```
python docker_build_test.py kafka/test --image-tag=3.6.0 --image-type=jvm --kafka-url=https://archive.apache.org/dist/kafka/3.6.0/kafka_2.13-3.6.0.tgz
```

Bulding image and running tests using github actions
----------------------------------------------------
This is the recommended way to build, test and get a CVE report for the docker image.
Just choose the image type and provide kafka url to `Docker Build Test` workflow. It will generate a test report and CVE report that can be shared with the community.

kafka-url - This is the url to download kafka tarball from. For example kafka tarball url from (https://archive.apache.org/dist/kafka). For building RC image this will be an RC tarball url.

image-type - This is the type of image that we intend to build. This will be dropdown menu type selection in the workflow. `jvm` image type is for official docker image (to be hosted on apache/kafka) as described in [KIP-975](https://cwiki.apache.org/confluence/display/KAFKA/KIP-975%3A+Docker+Image+for+Apache+Kafka)

Example command:-
To build and test a jvm image type ensuring kafka to be containerised should be https://archive.apache.org/dist/kafka/3.6.0/kafka_2.13-3.6.0.tgz (it is recommended to use scala 2.13 binary tarball), following inputs in github actions workflow are recommended.
```
image_type: jvm
kafka_url: https://archive.apache.org/dist/kafka/3.6.0/kafka_2.13-3.6.0.tgz
```

Creating a release
------------------
- `docker_release.py` script builds a multi-architecture image and pushes it to provided docker registry.
- Ensure you are logged in to the docker registry before triggering the script.
- kafka binary tarball url along with image name (in the format `<registry>/<namespace>/<image_name>:<image_tag>`) and type is needed to build the image. For detailed usage description check `python docker_release.py --help`.

Example command:-
To push an image named test under kafka dockerhub namespace with 3.6.0 tag and jvm image type ensuring kafka to be containerised should be https://archive.apache.org/dist/kafka/3.6.0/kafka_2.13-3.6.0.tgz (it is recommended to use scala 2.13 binary tarball), following command can be used. (Make sure you have push access to the docker repo)
```
# kafka/test is an example repo. Please replace with the docker hub repo you have push access to.

python docker_release.py kafka/test:3.6.0 --kafka-url https://archive.apache.org/dist/kafka/3.6.0/kafka_2.13-3.6.0.tgz
```

Please note that we use docker buildx for preparing the multi-architecture image and pushing it to docker registry. It's possible to encounter build failures because of buildx. Please retry the command in case some buildx related error occurs.

Promoting a release
-------------------
`docker_promote.py` provides an interactive way to pull an RC Docker image and promote it to required dockerhub repo.

Using the image in a docker container
-------------------------------------
- The image uses the kafka downloaded from provided kafka url
- The image can be run in a container in default mode by running
`docker run -p 9092:9092 <image-name:tag>`
- Default configs run kafka in kraft mode with plaintext listners on 9092 port.
- Once user provided config properties are provided default configs will get replaced.
- User can provide kafka configs following two ways:-
    - By mounting folder containing property files
        - Mount the folder containing kafka property files to `/mnt/shared/config`
        - These files will replace the default config files
    - Using environment variables
        - Kafka properties defined via env variables will override properties defined in file input
        - If properties are provided via environment variables only, default configs will be replaced by user provided properties
        - Input format for env variables:-
            - Replace . with _
            - Replace _ with __(double underscore)
            - Replace - with ___(triple underscore)
            - Prefix the result with KAFKA_
            - Examples:
                - For abc.def, use KAFKA_ABC_DEF
                - For abc-def, use KAFKA_ABC___DEF
                - For abc_def, use KAFKA_ABC__DEF
- Hence order of precedence of properties is the following:-
    - Env variable (highest)
    - File input
    - Default configs (only when there is no user provided config)
- Any env variable that is commonly used in starting kafka(for example, CLUSTER_ID) can be supplied to docker container and it will be available when kafka starts

Steps to release docker image
-----------------------------
- Make sure you have executed `release.py` script to prepare RC tarball in apache sftp server.
- Use the RC tarball url (make sure you choose scala 2.13 version) as input kafka url to build docker image and run sanity tests.
- Trigger github actions workflow using the RC branch, provide RC tarball url as kafka url.
- This will generate test report and CVE report for docker images.
- If the reports look fine, RC docker image can be built and published.
- Execute `docker_release.py` script to build and publish RC docker image in your dockerhub account.
- Share the RC docker image, test report and CVE report with the community in RC vote email.
- Once approved and ready, take help from someone in PMC to trigger `docker_promote.py` script and promote the RC docker image to apache/kafka dockerhub repo
