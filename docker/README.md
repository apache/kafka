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

Creating a release
------------------
`docker_release.py` provides an interactive way to build multi arch image and publish it to a docker registry.

Promoting a release
-------------------
`docker_promote.py` provides an interactive way to pull an RC Docker image and promote it to required dockerhub repo.

Using the image in a docker container
-------------------------------------
- The image uses the kafka downloaded from provided kafka url
- The image can be run in a container in default mode by running
`docker run <image-name:tag> -p 9092:9092`
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
- Hence order of precedence of properties is the follwing:-
    - Env variable (highest)
    - File input
    - Default (lowest)
- Any env variable that is commonly used in starting kafka(for example, CLUSTER_ID) can be supplied to docker container and it will be available when kafka starts
