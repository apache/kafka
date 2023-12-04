Docker Images
=============

This directory contains docker image for Kafka.
The scripts take a url containing kafka as input and generate the respective docker image.
There are interactive python scripts to release the docker image and promote a release candidate.

Local Setup
-----------
Make sure you have python (>= 3.7.x) and java (>= 17) installed before running the tests and scripts.

Run `pip install -r requirements.txt` to get all the requirements for running the scripts.

Bulding image and running tests locally
---------------------------------------
- `docker_build_test.py` script builds and tests the docker image.
- kafka binary tarball url along with image name, tag and type is needed to build the image. For detailed usage description check `python docker_build_test.py --help`.
- Sanity tests for the docker image are present in test/docker_sanity_test.py.
- By default image will be built and tested, but if you only want to build the image, pass `-b` flag and if you only want to test the given image pass `-t` flag.
- An html test report will be generated after the tests are executed containing the results.

Bulding image and running tests using github actions
----------------------------------------------------
This is the recommended way to build, test and get a CVE report for the docker image.
Just choose the image type and provide kafka url to `Docker build test` workflow. It will generate a test report and CVE report that can be shared to the community.

kafka-url - This is the url to download kafka tarball from. For example kafka tarball url from (https://archive.apache.org/dist/kafka). For building RC image this will be an RC tarball url.

image-type - This is the type of image that we intend to build. This will be dropdown menu type selection in the workflow. `jvm` image type is for official docker image (to be hosted on apache/kafka) as described in [KIP-975](https://cwiki.apache.org/confluence/display/KAFKA/KIP-975%3A+Docker+Image+for+Apache+Kafka)


Creating a release
------------------
- `docker_release.py` script builds a multi architecture image and pushes it to provided docker registry.
- Ensure you are logged in to the docker registry before triggering the script.
- kafka binary tarball url along with image name (in the format `<registry>/<namespace>/<image_name>:<image_tag>`) and type is needed to build the image. For detailed usage description check `python docker_release.py --help`.

Promoting a release
-------------------
`docker_promote.py` provides an interactive way to pull an RC Docker image and promote it to required dockerhub repo.

Using the image in a docker container
-------------------------------------
- The image uses the kafka downloaded from provided kafka url
- The image can be run in a container in default mode by running
`docker run -p 9092:9092 <image-name:tag>`
- Default configs run kafka in kraft mode with plaintext listners on 9092 port.
- Default configs can be overriden by user using 2 ways:-
    - By mounting folder containing property files
        - Mount the folder containing kafka property files to `/mnt/shared/config`
        - These files will override the default config files
    - Using environment variables
        - Kafka properties defined via env variables will override properties defined in file input
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
    - Default (lowest)
- Any env variable that is commonly used in starting kafka(for example, CLUSTER_ID) can be supplied to docker container and it will be available when kafka starts

Steps to release docker image
-----------------------------
- Make sure you have executed release.py script to prepare RC tarball in apache sftp server.
- Use the RC tarball url as input kafka url to build docker image and run sanity tests.
- Trigger github actions workflow using the RC branch, provide RC tarball url as kafka url.
- This will generate test report and CVE report for docker images.
- If the reports look fine, RC docker image can be built and published.
- Execute `docker_release.py` script to build and publish RC docker image in your own dockerhub account.
- Share the RC docker image, test report and CVE report with the community in RC vote email.
- Once approved and ready, take help from someone in PMC to trigger `docker_promote.py` script and promote the RC docker image to apache/kafka dockerhub repo
