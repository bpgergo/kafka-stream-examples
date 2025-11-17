# Candlestick Processor - Deployment Guide

## Prerequisites

### For Minikube Testing
- Minikube installed: `brew install minikube` (macOS) or [Install Guide](https://minikube.sigs.k8s.io/docs/start/)
- Docker installed
- kubectl installed

### For Production
- Kubernetes cluster (EKS, GKE, AKS, or on-premise)
- kubectl configured to access your cluster
- Container registry (Docker Hub, ECR, GCR, etc.)

## Quick Start with Minikube

### 1. Start Minikube
```bash
# Start with sufficient resources
minikube start --cpus=4 --memory=8192 --disk-size=20g

# Enable registry addon (optional, for local images)
minikube addons enable registry
```

### 2. Build the Docker Image
```bash
# Build the application
mvn clean package

# Build Docker image
docker build -t candlestick-processor:1.0.0 .

# Load image into Minikube (if not using registry)
minikube image load candlestick-processor:1.0.0
```

### 3. Deploy Kafka Cluster
```bash
# Create namespace
kubectl create namespace kafka-streams

# Deploy Zookeeper and Kafka (3 brokers)
kubectl apply -f kafka-cluster.yaml

# Wait for Kafka to be ready (this may take 2-3 minutes)
kubectl wait --for=condition=ready pod -l app=kafka -n kafka-streams --timeout=300s
```

### 4. Create Topics
```bash
# Get a Kafka pod name
KAFKA_POD=$(kubectl get pods -n kafka-streams -l app=kafka -o jsonpath='{.items[0].metadata.name}')

# Create input topic with 6 partitions
kubectl exec -n kafka-streams $KAFKA_POD -- kafka-topics \
  --create \
  --topic streams-candlestick-input \
  --partitions 6 \
  --replication-factor 3 \
  --bootstrap-server kafka-0.kafka-service.kafka-streams.svc.cluster.local:9092

# Create output topic
kubectl exec -n kafka-streams $KAFKA_POD -- kafka-topics \
  --create \
  --topic streams-candlestick-processor-output \
  --partitions 6 \
  --replication-factor 3 \
  --bootstrap-server kafka-0.kafka-service.kafka-streams.svc.cluster.local:9092

# Create DLQ topic
kubectl exec -n kafka-streams $KAFKA_POD -- kafka-topics \
  --create \
  --topic streams-candlestick-dlq \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server kafka-0.kafka-service.kafka-streams.svc.cluster.local:9092

# Verify topics
kubectl exec -n kafka-streams $KAFKA_POD -- kafka-topics \
  --list \
  --bootstrap-server kafka-0.kafka-service.kafka-streams.svc.cluster.local:9092
```

### 5. Deploy Candlestick Processor
```bash
# Deploy the application (3 replicas)
kubectl apply -f kubernetes-manifests.yaml

# Check deployment status
kubectl get pods -n kafka-streams -l app=candlestick-processor

# View logs
kubectl logs -n kafka-streams -l app=candlestick-processor -f
```

### 6. Test the Application

**Producer test data:**
```bash
# Get Kafka pod
KAFKA_POD=$(kubectl get pods -n kafka-streams -l app=kafka -o jsonpath='{.items[0].metadata.name}')

# Start a producer
kubectl exec -it -n kafka-streams $KAFKA_POD -- kafka-console-producer \
  --topic streams-candlestick-input \
  --bootstrap-server kafka-0.kafka-service.kafka-streams.svc.cluster.local:9092

# Enter some test prices (type these one by one):
# 150.25
# 151.30
# 149.80
# 152.00
# (Press Ctrl+C to exit)
```

**Consume output:**
```bash
# Start a consumer to see candlestick output
kubectl exec -it -n kafka-streams $KAFKA_POD -- kafka-console-consumer \
  --topic streams-candlestick-processor-output \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" => " \
  --bootstrap-server kafka-0.kafka-service.kafka-streams.svc.cluster.local:9092
```

### 7. Monitor the Application
```bash
# Check pod status
kubectl get pods -n kafka-streams

# View application logs
kubectl logs -n kafka-streams -l app=candlestick-processor --tail=100

# Describe deployment
kubectl describe deployment candlestick-processor -n kafka-streams

# Check resource usage
kubectl top pods -n kafka-streams

# View events
kubectl get events -n kafka-streams --sort-by='.lastTimestamp'
```

## Local Development (Single Broker)

For local testing with a single Kafka broker:

### 1. Update ConfigMap
Edit `kubernetes-manifests.yaml` and change:
```yaml
REPLICATION_FACTOR: "1"
NUM_STANDBY_REPLICAS: "0"
```

### 2. Deploy Single Kafka Broker
```bash
# Reduce Kafka replicas to 1
kubectl scale statefulset kafka -n kafka-streams --replicas=1
kubectl scale statefulset zookeeper -n kafka-streams --replicas=1

# Create topics with replication factor 1
kubectl exec -n kafka-streams kafka-0 -- kafka-topics \
  --create \
  --topic streams-candlestick-input \
  --partitions 6 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

## Testing Resilience

### Test Pod Failure
```bash
# Delete a pod to test recovery
kubectl delete pod -n kafka-streams -l app=candlestick-processor | head -1

# Watch pods recover
kubectl get pods -n kafka-streams -l app=candlestick-processor -w
```

### Test Partition Rebalancing
```bash
# Scale up
kubectl scale deployment candlestick-processor -n kafka-streams --replicas=5

# Scale down
kubectl scale deployment candlestick-processor -n kafka-streams --replicas=2

# Watch rebalancing in logs
kubectl logs -n kafka-streams -l app=candlestick-processor -f | grep -i rebalance
```

### Test Exactly-Once Processing
```bash
# Send duplicate messages and verify no duplicate candlesticks
# Producer with idempotent writes ensures exactly-once
```

## Configuration

### Environment Variables
You can override configuration via ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: candlestick-processor-config
  namespace: kafka-streams
data:
  BOOTSTRAP_SERVERS: "kafka-service:9092"
  NUM_STREAM_THREADS: "4"  # Increase for higher throughput
  REPLICATION_FACTOR: "3"
  NUM_STANDBY_REPLICAS: "1"
```

Apply changes:
```bash
kubectl apply -f kubernetes-manifests.yaml
kubectl rollout restart deployment candlestick-processor -n kafka-streams
```

## Troubleshooting

### Pods not starting
```bash
kubectl describe pod -n kafka-streams <pod-name>
kubectl logs -n kafka-streams <pod-name>
```

### Kafka connection issues
```bash
# Check Kafka service
kubectl get svc -n kafka-streams

# Test connectivity from processor pod
kubectl exec -it -n kafka-streams <processor-pod> -- sh
# Inside pod:
# nc -zv kafka-service 9092
```

### State store issues
```bash
# Check PVC
kubectl get pvc -n kafka-streams

# Check volume mounts
kubectl describe pod -n kafka-streams <pod-name>
```

## Cleanup

```bash
# Delete application
kubectl delete -f kubernetes-manifests.yaml

# Delete Kafka cluster
kubectl delete -f kafka-cluster.yaml

# Delete namespace (removes everything)
kubectl delete namespace kafka-streams

# Stop Minikube
minikube stop
```

## Production Considerations

1. **Resource Limits**: Adjust CPU/memory based on load
2. **Storage**: Use faster storage class (SSD) for state stores
3. **Monitoring**: Integrate with Prometheus/Grafana
4. **Alerting**: Set up alerts for pod failures, lag, errors
5. **Backup**: Consider state store backups (via changelog topics)
6. **Security**: Enable SASL/SSL for Kafka
7. **Network Policies**: Restrict pod-to-pod communication
8. **Pod Disruption Budgets**: Ensure minimum replicas during updates

## Metrics and Monitoring

Kafka Streams exposes JMX metrics. To enable:

1. Add JMX exporter to Docker image
2. Create ServiceMonitor for Prometheus
3. Import Grafana dashboards for Kafka Streams

Example metrics to monitor:
- `kafka.streams:type=stream-metrics,client-id=*` - Application metrics
- `kafka.streams:type=stream-task-metrics` - Task-level metrics
- Lag monitoring via consumer groups