# Two separate FlinkDeployments — isolated failure domains.
# Each job gets its own JobManager + TaskManager (application mode).

resource "kubernetes_manifest" "customer_aggregation" {
  manifest = {
    apiVersion = "flink.apache.org/v1beta1"
    kind       = "FlinkDeployment"
    metadata = {
      name      = "customer-aggregation"
      namespace = "processing"
    }
    spec = {
      image           = "klaflow-flink-jobs:latest"
      imagePullPolicy = "Never"
      flinkVersion    = "v1_20"
      serviceAccount  = "flink"
      mode            = "standalone"
      flinkConfiguration = {
        "state.backend"                    = "rocksdb"
        "state.checkpoints.dir"            = "file:///tmp/flink-checkpoints/customer-agg"
        "execution.checkpointing.interval" = "60000"
        "taskmanager.numberOfTaskSlots"     = "1"
        "parallelism.default"               = "1"
      }
      jobManager = {
        resource = {
          memory = "1Gi"
          cpu    = 0.1
        }
      }
      taskManager = {
        resource = {
          memory = "1Gi"
          cpu    = 0.25
        }
      }
      job = {
        jarURI      = "local:///opt/flink/opt/flink-python-1.20.0.jar"
        entryClass  = "org.apache.flink.client.python.PythonDriver"
        args         = ["-pyclientexec", "/usr/bin/python3", "-py", "/opt/flink/jobs/customer_aggregation_job.py"]
        parallelism = 1
        upgradeMode = "stateless"
        state       = "running"
      }
      podTemplate = {
        spec = {
          containers = [{
            name = "flink-main-container"
            env = [
              { name = "KAFKA_BOOTSTRAP", value = "kafka.streaming.svc.cluster.local:9092" },
              { name = "KAFKA_TOPIC", value = "customer-events" },
              { name = "SCHEMA_REGISTRY_URL", value = "http://schema-registry.streaming.svc.cluster.local:8081" },
              { name = "CLICKHOUSE_HOST", value = "clickhouse.analytics.svc.cluster.local" },
              { name = "CLICKHOUSE_PORT", value = "8123" },
              { name = "CLICKHOUSE_USER", value = "klaflow" },
              { name = "CLICKHOUSE_PASSWORD", value = "klaflow-pass" },
              { name = "FLUSH_INTERVAL_MS", value = "10000" },
            ]
          }]
        }
      }
    }
  }
}

resource "kubernetes_manifest" "account_aggregation" {
  manifest = {
    apiVersion = "flink.apache.org/v1beta1"
    kind       = "FlinkDeployment"
    metadata = {
      name      = "account-aggregation"
      namespace = "processing"
    }
    spec = {
      image           = "klaflow-flink-jobs:latest"
      imagePullPolicy = "Never"
      flinkVersion    = "v1_20"
      serviceAccount  = "flink"
      mode            = "standalone"
      flinkConfiguration = {
        "state.backend"                    = "rocksdb"
        "state.checkpoints.dir"            = "file:///tmp/flink-checkpoints/account-agg"
        "execution.checkpointing.interval" = "60000"
        "taskmanager.numberOfTaskSlots"     = "1"
        "parallelism.default"               = "1"
      }
      jobManager = {
        resource = {
          memory = "1Gi"
          cpu    = 0.1
        }
      }
      taskManager = {
        resource = {
          memory = "1Gi"
          cpu    = 0.25
        }
      }
      job = {
        jarURI      = "local:///opt/flink/opt/flink-python-1.20.0.jar"
        entryClass  = "org.apache.flink.client.python.PythonDriver"
        args         = ["-pyclientexec", "/usr/bin/python3", "-py", "/opt/flink/jobs/account_aggregation_job.py"]
        parallelism = 1
        upgradeMode = "stateless"
        state       = "running"
      }
      podTemplate = {
        spec = {
          containers = [{
            name = "flink-main-container"
            env = [
              { name = "KAFKA_BOOTSTRAP", value = "kafka.streaming.svc.cluster.local:9092" },
              { name = "KAFKA_TOPIC", value = "customer-events" },
              { name = "SCHEMA_REGISTRY_URL", value = "http://schema-registry.streaming.svc.cluster.local:8081" },
              { name = "CLICKHOUSE_HOST", value = "clickhouse.analytics.svc.cluster.local" },
              { name = "CLICKHOUSE_PORT", value = "8123" },
              { name = "CLICKHOUSE_USER", value = "klaflow" },
              { name = "CLICKHOUSE_PASSWORD", value = "klaflow-pass" },
              { name = "FLUSH_INTERVAL_MS", value = "10000" },
            ]
          }]
        }
      }
    }
  }
}
