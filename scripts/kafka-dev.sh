#!/bin/sh
set -eu

KAFKA_BIN="/opt/homebrew/opt/kafka/bin"
KAFKA_CONFIG="/opt/homebrew/etc/kafka/server.properties"
BOOTSTRAP="localhost:9092"
LOG="${TMPDIR:-/tmp}/kafkamitter-kafka.log"

usage() {
    echo "usage: $0 start | stop | status | seed"
    exit 1
}

case "${1:-}" in
    start)
        if "$KAFKA_BIN/kafka-topics" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
            echo "broker already answers on $BOOTSTRAP"
            exit 0
        fi
        nohup "$KAFKA_BIN/kafka-server-start" "$KAFKA_CONFIG" >"$LOG" 2>&1 &
        echo "starting broker, log: $LOG"
        i=0
        while [ $i -lt 30 ]; do
            if "$KAFKA_BIN/kafka-topics" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
                echo "broker is up on $BOOTSTRAP"
                exit 0
            fi
            i=$((i + 1))
            sleep 1
        done
        echo "broker did not answer in 30 s, see $LOG"
        exit 1
        ;;
    stop)
        "$KAFKA_BIN/kafka-server-stop" || true
        echo "stop requested"
        ;;
    status)
        "$KAFKA_BIN/kafka-topics" --bootstrap-server "$BOOTSTRAP" --list
        ;;
    seed)
        "$KAFKA_BIN/kafka-topics" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic orders --partitions 3
        seq 1 500 \
            | sed 's/.*/order-&|{"id":&,"status":"NEW","customer":{"name":"c&","tier":"gold"},"amount":&.5}/' \
            | "$KAFKA_BIN/kafka-console-producer" --bootstrap-server "$BOOTSTRAP" --topic orders \
                --property parse.key=true --property key.separator='|'
        "$KAFKA_BIN/kafka-console-consumer" --bootstrap-server "$BOOTSTRAP" --topic orders --group billing --from-beginning --max-messages 200 >/dev/null
        "$KAFKA_BIN/kafka-consumer-groups" --bootstrap-server "$BOOTSTRAP" --describe --group billing
        ;;
    *)
        usage
        ;;
esac
