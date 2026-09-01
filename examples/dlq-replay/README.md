# DLQ Replay Example

Collector config for replaying failed records from Kafka DLQ back to OpenSearch.

## How it works

1. **Kafka receiver** reads from DLQ topic with a unique `group_id` per replay run.
2. **Filter processor** (optional) selects only permanent errors or a time range.
3. **Attributes processor** strips stamped `opensearch.error.*` attributes.
4. **OpenSearch exporter** re-indexes with `bulk_action: index` (upsert) and `document_id_from_attribute` set to the stamped `opensearch._id` for idempotent replay.

## Usage

### Run as Kubernetes Job

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: dlq-replay-001
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: collector
          image: otelcontribcol:latest
          command: ["/otelcontribcol"]
          args: ["--config=/etc/collector/config.yaml"]
          volumeMounts:
            - name: config
              mountPath: /etc/collector
          env:
            - name: KAFKA_USERNAME
              valueFrom:
                secretKeyRef:
                  name: kafka-creds
                  key: username
            - name: KAFKA_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: kafka-creds
                  key: password
            - name: OPENSEARCH_USERNAME
              valueFrom:
                secretKeyRef:
                  name: opensearch-creds
                  key: username
            - name: OPENSEARCH_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: opensearch-creds
                  key: password
      volumes:
        - name: config
          configMap:
            name: dlq-replay-config
```

Delete the Job when Kafka consumer lag hits zero.

### Adjust filters

To replay only specific error types:

```yaml
filter/permanent_only:
  logs:
    log_record:
      - 'attributes["opensearch.error.type"] == "mapper_parsing_exception"'
```

To replay a time range:

```yaml
filter/time_range:
  logs:
    log_record:
      - 'time_unix_nano >= 1672531200000000000 and time_unix_nano < 1672617600000000000'
```

### Inspect before replay

Run with a `debug` exporter instead of `opensearch` to preview what will be replayed:

```yaml
exporters:
  debug:
    verbosity: normal

service:
  pipelines:
    logs:
      receivers: [kafka]
      processors: [filter/permanent_only]
      exporters: [debug]
```

## Prerequisites

- DLQ pipeline must stamp `opensearch._id` on records before the DLQ Kafka exporter (see main ingester chart).
- OpenSearch index mapping must allow the fields being replayed.
