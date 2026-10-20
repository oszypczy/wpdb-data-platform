# Kafka Connect for Debezium with the Confluent AVRO converter.
#
# quay.io/debezium/connect >= 2.0 ships the Debezium connectors but not the
# Confluent converter, so this image adds the Confluent Hub package
# kafka-connect-avro-converter (the zip contains the converter and all its
# dependencies) under the plugin path /kafka/connect. The CP version must match
# the broker and Schema Registry images (docs/design/08-operations.md §1).
ARG DEBEZIUM_VERSION=3.6.3.Final
FROM quay.io/debezium/connect:${DEBEZIUM_VERSION}
ARG CP_VERSION=8.3.2

USER root
RUN set -eux; \
    curl -fsSL -o /tmp/avro-converter.zip \
      "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/${CP_VERSION}/confluentinc-kafka-connect-avro-converter-${CP_VERSION}.zip"; \
    mkdir -p /kafka/connect/confluent-avro-converter; \
    unzip -q /tmp/avro-converter.zip -d /tmp/avro-converter; \
    mv /tmp/avro-converter/confluentinc-kafka-connect-avro-converter-${CP_VERSION}/lib/*.jar /kafka/connect/confluent-avro-converter/; \
    rm -rf /tmp/avro-converter /tmp/avro-converter.zip; \
    chown -R kafka:kafka /kafka/connect/confluent-avro-converter
USER kafka
