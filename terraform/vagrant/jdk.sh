
#!/bin/bash


# Setup Java
export DEFAULT_JDK_VERSION=${DEFAULT_JDK_VERSION:-8}

# where to download jdk
path_to_jdk_cache() {
    jdk_version=$1
    echo "/var/cache/jdk/jdk-${jdk_version}-linux-64.tar.gz"
}

# download jdk tgz to jdk cache
fetch_jdk_tgz() {

    jdk_version=$1
    jdk_s3_url=$2
    url_path=$3
    jdk_path=$(path_to_jdk_cache $jdk_version)
    if [ ! -e "${jdk_path}" ]; then
        mkdir -p "$(dirname ${jdk_path})"
        # Grab a copy of the JDK since it has moved and original downloader won't work
        if ! curl -s -L "${jdk_s3_url}/${url_path}" -o "${jdk_path}";then
            echo "FAILED to download jdk for version ${jdk_version}" >&2
            exit 1
        fi
    fi
}

install_jdk() {
  if [[ $DEFAULT_JDK_VERSION -lt 8 ]]; then
      echo "JDK 7 is not supported by Confluent Platform"
      exit 1
  fi

  echo "Running generic jdk installation based on s3 stored jdk"
  jdk_version=$DEFAULT_JDK_VERSION
  jdk_s3_url=https://s3-us-west-2.amazonaws.com/kafka-packages
  url_path=''
  if [ $(arch) = "aarch64" ]; then
      jdk_s3_url=https://s3-us-west-2.amazonaws.com/confluent-packaging-tools
      if [[ $DEFAULT_JDK_VERSION = 8 ]]; then
          jdk_version=8u372
          url_path="zulu8.70.0.23-ca-jdk8.0.372-linux_aarch64.tar.gz"
      elif [[ $DEFAULT_JDK_VERSION = 11 ]]; then
          jdk_version=11.0.15_10
          url_path="OpenJDK11U-jdk_aarch64_linux_hotspot_${jdk_version}.tar.gz"
      elif [[ $DEFAULT_JDK_VERSION = 17 ]]; then
          jdk_version=17.0.4.1_1
          url_path="OpenJDK17U-jdk_aarch64_linux_hotspot_${jdk_version}.tar.gz"
      elif [[ $DEFAULT_JDK_VERSION = 18 ]]; then
          jdk_version=18.0
          # below url path is hardcoded as java version prefix was not present for java 18
          url_path="openjdk-18_linux-aarch64_bin.tar.gz"
      else
          url_path="OpenJDK17U-jdk_aarch64_linux_hotspot_${jdk_version}.tar.gz"
      fi
  else
      if [[ $DEFAULT_JDK_VERSION = 8 ]]; then
          jdk_version=8u202
      elif [[ $DEFAULT_JDK_VERSION = 11 ]]; then
          jdk_version=11.0.2
      elif [[ $DEFAULT_JDK_VERSION = 18 ]]; then
          jdk_version=18.0.2
      elif [[ $DEFAULT_JDK_VERSION = 21 ]]; then
          jdk_version=21.0.1
      fi
      url_path="jdk-${jdk_version}-linux-x64.tar.gz"
  fi

  echo "===> Installing JDK..."
  rm -rf "$DEFAULT_JDK_VERSION"
  mkdir -p /opt/jdk
  cd /opt/jdk
  fetch_jdk_tgz $jdk_version $jdk_s3_url $url_path
  # redhat needs sudo, ubuntu is ok without it, keep sudo for interoperability
  sudo tar zxf "$(path_to_jdk_cache $jdk_version)"
  mv "$(echo *jdk*)" "$DEFAULT_JDK_VERSION"
  for bin in /opt/jdk/$DEFAULT_JDK_VERSION/bin/* ; do
    name=$(basename $bin)
    update-alternatives --install /usr/bin/$name $name $bin 1091 && update-alternatives --set $name $bin
  done
  echo -e "export JAVA_HOME=/opt/jdk/${DEFAULT_JDK_VERSION}\nexport PATH=\$PATH:\$JAVA_HOME/bin" > /etc/profile.d/jdk.sh
  # not sure this linking is even required, it was present on redhat but not on ubuntu
  # might be related to the fact that we used to install default java and then unpack our jdk tar on top of it
  # keep it to avoid any potential problems
  # TODO: reevaluate for jeeves
  mkdir -p /usr/lib/jvm/
  ln -snf /opt/jdk/${DEFAULT_JDK_VERSION} /usr/lib/jvm/default-java
  echo "JDK installed: $(javac -version 2>&1)"
}
