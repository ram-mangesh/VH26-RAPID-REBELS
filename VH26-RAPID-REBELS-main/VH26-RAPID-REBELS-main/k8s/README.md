# Kubernetes Setup for DataStream

## Quick Start

### 1. Enable Kubernetes in Docker Desktop
- Open Docker Desktop → Settings → Kubernetes
- Check "Enable Kubernetes"
- Click "Apply & Restart"
- Wait for cluster to initialize (2-3 minutes)

### 2. Verify kubectl is connected
```powershell
kubectl cluster-info
kubectl get nodes
```

### 3. Deploy all services
```powershell
.\deploy-k8s.ps1
```

### 4. Monitor deployment
```powershell
kubectl get pods -n datastream -w
kubectl get svc -n datastream
```

## Health Monitoring

### Quick Health Check
```powershell
.\check-health.ps1
```

### Kubernetes Health Probes

All services have **liveness** and **readiness** probes configured:

| Service | Liveness Probe | Readiness Probe |
|---------|---------------|-----------------|
| Kafka | `/opt/kafka/bin/kafka-topics.sh --list` | Same as liveness |
| ClickHouse | `http://:8123/ping` | Same as liveness |
| Prometheus | `http://:9090/-/healthy` | `http://:9090/-/ready` |
| Grafana | `http://:3000/api/health` | Same as liveness |
| Go Producer | `http://:8080/health` | Same as liveness |
| Intelligence Pipeline | `http://:8081/api/state` | Same as liveness |
| Frontend | `http://:80/` | Same as liveness |

### Probe Configuration

**Liveness Probe**: Restarts container if failed (auto-recovery)
**Readiness Probe**: Removes from service until ready (no traffic)

### Autoscaling (Optional)

```powershell
# Enable HPA for go-producer
kubectl autoscale deployment go-producer -n datastream --min=2 --max=5 --cpu-percent=70
```

## Services & Ports

| Service | Internal Port | External | Health Endpoint |
|---------|--------------|----------|-----------------|
| Kafka | 9092 | localhost:9094 | topics list |
| ClickHouse | 8123, 9000 | localhost:8123 | /ping |
| Prometheus | 9090 | localhost:9090 | /-/healthy |
| Grafana | 3000 | localhost:3001 | /api/health |
| Go Producer | 8080 | localhost:8080 | /health |
| Stream Processor | 8081 | N/A | pgrep |
| Intelligence Pipeline | 8081 | localhost:8081 | /api/state |
| Frontend | 80 | localhost:3002 | / |

## Monitoring with Prometheus

All services are scraped by Prometheus every 10s:
```
http://localhost:9090/targets
```

## Kubernetes Dashboard

```powershell
kubectl proxy
# Open: http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/
```

## Scaling

```powershell
# Scale deployment
kubectl scale deployment go-producer -n datastream --replicas=3

# View replica count
kubectl get replicasets -n datastream
```

## Logs & Debugging

```powershell
# View pod logs
kubectl logs -n datastream -l app=go-producer -f

# View specific pod
kubectl logs -n datastream <pod-name>

# Exec into pod
kubectl exec -it -n datastream <pod-name> -- /bin/bash

# Describe pod (events, probes)
kubectl describe pod -n datastream <pod-name>
```

## Cleanup

```powershell
# Delete all resources
kubectl delete namespace datastream

# Or delete individual resources
kubectl delete -f k8s/
```

## Health Monitor Dashboard

Open `k8s/health-monitor.html` in browser for visual health monitoring.
Auto-refreshes every 10 seconds.

## Production Considerations

1. **Persistent Volumes**: Use cloud provider PVs instead of local storage
2. **Secrets**: Store passwords in Kubernetes Secrets
3. **Ingress**: Use proper TLS certificates
4. **HPA**: Enable horizontal pod autoscaling
5. **Network Policies**: Restrict inter-service communication
6. **Resource Quotas**: Limit namespace resource usage
7. **Backup**: Set up ClickHouse backups
8. **Alerting**: Configure Prometheus AlertManager
