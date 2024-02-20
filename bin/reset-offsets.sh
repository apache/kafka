#!/bin/bash
BROKER=$1
TOPIC=$2
GROUP=$3
TMP=PART_$3
reset_offset() {
  source kafka-consumer-groups.sh \
  --bootstrap-server $BROKER \
  --reset-offsets \
  --group $GROUP \
  --topic $TOPIC:$1 \
  --to-offset $2
}
source kafka-consumer-groups.sh \
--bootstrap-server $BROKER \
--describe \
--group $GROUP \
| awk {'print $3'} > $TMP;
sed -i "1,2d" $TMP;
for partition in $(cat $TMP);
  do reset_offset $partition 0;
  done;
exit 0;
