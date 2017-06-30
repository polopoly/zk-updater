# ZkUdater

This tool will update the Couchbase Kafka Connector (2.0.0) ZooKeeper state to match the most recent failover log entries from Couchbase.

## Build

mvn clean compile assembly:single

## Run

java -jar target/zk-updater-jar-with-dependencies.jar localhost localhost cmbucket cmpasswd couchbase-kafka-connector2
