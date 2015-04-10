#!/bin/bash

# script directory
SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"

if [ "$#" -ne 6 ]; then
    echo "usage: startservers.sh <num servers> <rmi port> <num servers to fail> <time to fail servers (seconds)> <time for servers to restart (seconds)> <time to simulate system (seconds)>"
    exit;
fi

OUTPUT_FILE="$SCRIPT_DIR/../serverlogs/server.output"
echo "Writing simulation output to $OUTPUT_FILE"


NUM_SERVERS="$1"
echo "Simulating $NUM_SERVERS servers"
echo "NUM_SERVERS=$NUM_SERVERS" > $OUTPUT_FILE

RMI_PORT="$2"

NUM_TO_FAIL="$3"
echo "Failing $NUM_TO_FAIL servers at a time"
echo "NUM_TO_FAIL=$NUM_TO_FAIL" >> $OUTPUT_FILE

TIME_TO_FAIL="$4"
echo "Waiting $TIME_TO_FAIL seconds between failiures"
echo "TIME_TO_FAIL=$TIME_TO_FAIL" >> $OUTPUT_FILE

TIME_TO_RESTART="$5"
echo "Failed servers restart after $TIME_TO_RESTART seconds"
echo "TIME_TO_RESTART=$TIME_TO_RESTART" >> $OUTPUT_FILE

TIME_TO_SIMULATE="$6"
echo "Simulation will last $TIME_TO_SIMULATE seconds"
echo "TIME_TO_SIMULATE=$TIME_TO_SIMULATE" >> $OUTPUT_FILE

LOG_DIR="$SCRIPT_DIR/../serverlogs"
echo "Reading log file from $LOG_DIR"
CONFIG_DIR="$SCRIPT_DIR/../serverlogs"
echo "Reading config file from $CONFIG_DIR"

echo "Restarting rmiregistry"

RMI_PID=`$SCRIPT_DIR/pidof rmiregistry`

if [ -n "$RMI_PID" ] 
then
    echo "Killing old rmiregistry ($RMI_PID)"
    kill -9 "$RMI_PID"
fi

cd "$SCRIPT_DIR"
rmiregistry "$RMI_PORT" &
echo "Waiting for rmiregistry to start."
sleep 5

declare -a SERVER_PIDS

for (( id=1; id<=$NUM_SERVERS; id++ ))
do
    # initialize servers' log and config files
    cp "$LOG_DIR/init.log" "$LOG_DIR/$id.log"
    cp "$CONFIG_DIR/init.config" "$CONFIG_DIR/$id.config"
    echo "NUM_SERVERS=$NUM_SERVERS" >> "$CONFIG_DIR/$id.config"
    java -classpath "$SCRIPT_DIR" -Djava.rmi.server.codebase=file://localhost/$SCRIPT_DIR/ edu.duke.raft.StartServer 1098 "$id" "$LOG_DIR" "$CONFIG_DIR" >> $OUTPUT_FILE &
    PID="$!"
    SERVER_PIDS[$id]="$PID"
    echo "Starting server S$id at $PID"
done

START=`date +%s`
while [ $(( $(date +%s) - $TIME_TO_SIMULATE )) -lt $START ] 
do
    echo "Going to sleep"
    sleep $TIME_TO_FAIL

    for (( failures=0; failures<$NUM_TO_FAIL; failures++ ))
    do
	let "id = 0"
	while [ -z  "${SERVER_PIDS[$id]}" ]
	do
	    id=$RANDOM
	    let "id %= $NUM_SERVERS"
	    let "id += 1"
	done

	echo "Failing S$id"
	kill -9 ${SERVER_PIDS[$id]}
	SERVER_PIDS[$id]=""
    done

    sleep $TIME_TO_RESTART

    for (( id=1; id<=$NUM_SERVERS; id++ ))
    do
	if [ -z "${SERVER_PIDS[$id]}" ] 
	then
	    java -classpath "$SCRIPT_DIR" -Djava.rmi.server.codebase=file://localhost/$SCRIPT_DIR/ edu.duke.raft.StartServer 1098 "$id" "$LOG_DIR" "$CONFIG_DIR"  >> $OUTPUT_FILE &
	    PID="$!"
	    SERVER_PIDS[$id]="$PID"
	    echo "Restarted server S$id at $PID"
	fi
    done
done

echo "Shutting down simulation"

for (( id=1; id<=$NUM_SERVERS; id++ ))
do
    echo "Shutting down server S$id"
    kill -9 ${SERVER_PIDS[$id]}
done
