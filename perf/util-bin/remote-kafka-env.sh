
REMOTE_KAFKA_HOME="~/kafka-perf"
REMOTE_KAFKA_LOG_DIR="$REMOTE_KAFKA_HOME/tmp/kafka-logs"
SIMULATOR_SCRIPT="$REMOTE_KAFKA_HOME/perf/run-simulator.sh"

REMOTE_KAFKA_HOST=`echo $REMOTE_KAFKA_LOGIN | cut -d @ -f 2`
REMOTE_SIM_HOST=`echo $REMOTE_SIM_LOGIN | cut -d @ -f 2`

# If we are running the broker on the same box, use the local interface.
KAFKA_SERVER=$REMOTE_KAFKA_HOST
if [[ "$REMOTE_KAFKA_HOST" == "$REMOTE_SIM_HOST" ]];
then
    KAFKA_SERVER="localhost"
fi


# todo: some echos
# todo: talkative sleep

function kafka_startup() {
    ssh $REMOTE_KAFKA_LOGIN "cd $REMOTE_KAFKA_HOME; ./bin/kafka-server-start.sh config/server.properties 2>&1 > kafka.out" &
    sleep 10
}


function kafka_cleanup() {
    ssh $REMOTE_KAFKA_LOGIN "cd $REMOTE_KAFKA_HOME; ./bin/kafka-server-stop.sh" &
    sleep 10
    ssh $REMOTE_KAFKA_LOGIN "rm -rf $REMOTE_KAFKA_LOG_DIR" &
    sleep 10
}
