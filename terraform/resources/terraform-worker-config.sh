# Check AMI_ID present or not as env variable if not then set
AMI_ID() {
  echo "ami-5189a661"
}

if [ -z "${AMI_ID}" ]; then
  echo "AMI_ID is not set. Setting it now."
  # Set the AMI_ID variable
  export AMI_ID=$(AMI_ID)
else
  echo "AMI_ID is already set to: $AMI_ID"
fi

export IPV6_SUPPORT="False"