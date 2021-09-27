#!/usr/bin/env bash
# shellcheck disable=SC2164
cd ~
cd /usr/lib/kafka

## Create topics
./bin/kafka-topics.sh --create \
    --replication-factor 1 \
    --partitions 2 \
    --topic readBitcoinsTransactions \
    --zookeeper  localhost:2181


## List created topics
./bin/kafka-topics.sh --list \
    --zookeeper localhost:2181