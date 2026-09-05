# Kubernetes Deployment Script for DataStream
# Run this to deploy all services with health monitoring

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  DataStream Kubernetes Deployment" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if kubectl is available
if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: kubectl not found. Please install kubectl first." -ForegroundColor Red
    exit 1
}

# Check cluster connection
Write-Host "Checking Kubernetes cluster connection..." -ForegroundColor Yellow
try {
    kubectl cluster-info
    Write-Host "Cluster connection OK" -ForegroundColor Green
    Write-Host ""
} catch {
    Write-Host "ERROR: Cannot connect to Kubernetes cluster" -ForegroundColor Red
    Write-Host "Please ensure Docker Desktop Kubernetes is enabled or a cluster is running" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "To enable Kubernetes in Docker Desktop:" -ForegroundColor Yellow
    Write-Host "1. Open Docker Desktop settings" -ForegroundColor Yellow
    Write-Host "2. Go to 'Kubernetes' tab" -ForegroundColor Yellow
    Write-Host "3. Check 'Enable Kubernetes'" -ForegroundColor Yellow
    Write-Host "4. Click 'Apply & Restart'" -ForegroundColor Yellow
    exit 1
}

# Create namespace
Write-Host "Creating namespace 'datastream'..." -ForegroundColor Yellow
kubectl apply -f k8s/namespace.yml
Write-Host ""

# Create ConfigMaps
Write-Host "Creating ConfigMaps..." -ForegroundColor Yellow
Write-Host "  - ClickHouse init scripts" -ForegroundColor Gray
Write-Host "  - Prometheus configuration" -ForegroundColor Gray
Write-Host "  - Grafana provisioning" -ForegroundColor Gray
Write-Host ""

# Apply ClickHouse init ConfigMap
$content = Get-Content "C:\Users\Riddhika\Downloads\vcet hackathon\DataStream-main\clickhouse\init\01-init.sql" -Raw
$base64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($content))
$configMapYaml = @"
apiVersion: v1
kind: ConfigMap
metadata:
  name: clickhouse-init
  namespace: datastream
data:
  01-init.sql: |
$(($content | ForEach-Object { "    $_" }) -join "`n")
"@
$configMapYaml | Out-File -FilePath "C:\Users\Riddhika\Downloads\vcet hackathon\k8s\clickhouse-init-cm.yml" -Encoding utf8
kubectl apply -f k8s/clickhouse-init-cm.yml
Write-Host "  ClickHouse init ConfigMap created" -ForegroundColor Green
Write-Host ""

# Create Prometheus ConfigMap
$promContent = Get-Content "C:\Users\Riddhika\Downloads\vcet hackathon\DataStream-main\prometheus\prometheus.yml" -Raw
$promConfigMap = @"
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
  namespace: datastream
data:
  prometheus.yml: |
$(($promContent | ForEach-Object { "    $_" }) -join "`n")
"@
$promConfigMap | Out-File -FilePath "C:\Users\Riddhika\Downloads\vcet hackathon\k8s\prometheus-cm.yml" -Encoding utf8
kubectl apply -f k8s/prometheus-cm.yml
Write-Host "  Prometheus ConfigMap created" -ForegroundColor Green
Write-Host ""

# Create Grafana ConfigMap
$grafanaProvisioning = Get-Content "C:\Users\Riddhika\Downloads\vcet hackathon\DataStream-main\grafana\provisioning\**\*" -Raw -ErrorAction SilentlyContinue
$grafanaConfigMap = @"
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-provisioning
  namespace: datastream
data:
  dashboards:
    dashboards.yaml: |
      apiVersion: 1
      providers:
      - name: 'default'
        orgId: 1
        folder: ''
        type: file
        disableDeletion: false
        editable: true
        options:
          path: /etc/grafana/provisioning/dashboards
          foldersFromFiles: true
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-datasources
  namespace: datastream
data:
  datasources.yaml: |
    apiVersion: 1
    datasources:
    - name: ClickHouse
      type: grafana-clickhouse-datasource
      access: proxy
      url: clickhouse:9000
      database: ecommerce
      isDefault: false
      editable: true
"@
$grafanaConfigMap | Out-File -FilePath "C:\Users\Riddhika\Downloads\vcet hackathon\k8s\grafana-cm.yml" -Encoding utf8
kubectl apply -f k8s/grafana-cm.yml
Write-Host "  Grafana ConfigMaps created" -ForegroundColor Green
Write-Host ""

# Build Docker images for K8s
Write-Host "Building Docker images..." -ForegroundColor Yellow
$services = @("go-producer", "stream-processor", "frontend", "intelligence-pipeline")
foreach ($svc in $services) {
    Write-Host "  Building $svc..." -ForegroundColor Gray
    docker compose -f DataStream-main/docker-compose.yml build $svc 2>&1 | Out-Null
    Write-Host "  ✓ $svc built" -ForegroundColor Green
}
Write-Host ""

# Load images into K8s (if using kind/minikube)
Write-Host "Checking K8s image registry..." -ForegroundColor Yellow
$clusterType = "unknown"
try {
    $clusterInfo = kubectl cluster-info | Select-String -Pattern "minikube|kind|k3s" -Quiet
    if ($clusterInfo) { $clusterType = "kind/minikube" }
} catch { $clusterType = "unknown" }

if ($clusterType -in @("kind", "minikube")) {
    Write-Host "Loading images into cluster..." -ForegroundColor Yellow
    foreach ($svc in $services) {
        $image = "datastream-main-$svc:latest"
        if ($clusterType -eq "kind") {
            kind load docker-image $image 2>&1 | Out-Null
        } else {
            minikube image load $image 2>&1 | Out-Null
        }
        Write-Host "  ✓ $image loaded" -ForegroundColor Green
    }
    Write-Host ""
}

# Apply all Kubernetes manifests
Write-Host "Deploying services to Kubernetes..." -ForegroundColor Yellow
Write-Host ""

Write-Host "  1/4 Applying infra services (Kafka, ClickHouse, Prometheus)..." -ForegroundColor Gray
kubectl apply -f k8s/kafka.yml -f k8s/clickhouse.yml -f k8s/prometheus-cm.yml 2>&1 | Out-Null
Write-Host "  ✓ Infra services deployed" -ForegroundColor Green

Write-Host "  2/4 Applying app services (Go Producer, Stream Processor)..." -ForegroundColor Gray
kubectl apply -f k8s/stream-services.yml 2>&1 | Out-Null
Write-Host "  ✓ App services deployed" -ForegroundColor Green

Write-Host "  3/4 Applying monitoring (Grafana, Intelligence Pipeline, Frontend)..." -ForegroundColor Gray
kubectl apply -f k8s/grafana.yml -f k8s/grafana-cm.yml -f k8s/app-services.yml 2>&1 | Out-Null
Write-Host "  ✓ Monitoring services deployed" -ForegroundColor Green

Write-Host "  4/4 Applying services & ingress..." -ForegroundColor Gray
kubectl apply -f k8s/services.yml 2>&1 | Out-Null
kubectl apply -f k8s/ingress.yml 2>&1 | Out-Null
Write-Host "  ✓ Network services deployed" -ForegroundColor Green

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Deployment Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Monitor deployment status:" -ForegroundColor Yellow
Write-Host "  kubectl get pods -n datastream -w" -ForegroundColor Cyan
Write-Host ""
Write-Host "Check health dashboard:" -ForegroundColor Yellow
Write-Host "  Open: k8s/health-monitor.html" -ForegroundColor Cyan
Write-Host ""
Write-Host "View service logs:" -ForegroundColor Yellow
Write-Host "  kubectl logs -n datastream -l app=<service-name> -f" -ForegroundColor Cyan
Write-Host ""
Write-Host "Scale a service:" -ForegroundColor Yellow
Write-Host "  kubectl scale deployment/<service-name> -n datastream --replicas=3" -ForegroundColor Cyan
Write-Host ""
Write-Host "Cleanup (delete all):" -ForegroundColor Yellow
Write-Host "  kubectl delete namespace datastream" -ForegroundColor Red
Write-Host ""
