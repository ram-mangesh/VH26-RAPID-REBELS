# Quick Health Check Script (works with or without Kubernetes)
# Run: .\check-health.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  DataStream Quick Health Check" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$services = @(
    @{
        Name = "ClickHouse"
        Url = "http://localhost:8123"
        Endpoint = "/ping"
        Type = "ping"
        Expected = "Ok"
        Description = "Analytics database"
    },
    @{
        Name = "Go Producer"
        Url = "http://localhost:8080"
        Endpoint = "/health"
        Type = "json"
        Expected = "ok"
        Description = "Event generator API"
    },
    @{
        Name = "Intelligence Pipeline"
        Url = "http://localhost:8081"
        Endpoint = "/api/state"
        Type = "json"
        Expected = "state"
        Description = "Adaptive processing dashboard"
    },
    @{
        Name = "Grafana"
        Url = "http://localhost:3001"
        Endpoint = "/api/health"
        Type = "json"
        Expected = "version"
        Description = "Monitoring dashboards"
    },
    @{
        Name = "Prometheus"
        Url = "http://localhost:9090"
        Endpoint = "/-/healthy"
        Type = "text"
        Expected = "Prometheus Server is Ready"
        Description = "Metrics collection"
    },
    @{
        Name = "Frontend"
        Url = "http://localhost:3002"
        Endpoint = "/"
        Type = "html"
        Expected = "html"
        Description = "React dashboard"
    },
    @{
        Name = "Kafka"
        Url = "http://localhost:9094"
        Endpoint = "/"
        Type = "tcp"
        Expected = "connected"
        Description = "Message broker"
    },
    @{
        Name = "Kafka UI"
        Url = "http://localhost:8090"
        Endpoint = "/"
        Type = "html"
        Expected = "html"
        Description = "Kafka management"
    }
)

$healthy = 0
$unhealthy = 0
$results = @()

foreach ($svc in $services) {
    Write-Host "Checking $($svc.Name)..." -NoNewline
    
    try {
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        
        if ($svc.Type -eq "tcp") {
            $tcp = New-Object System.Net.Sockets.TcpClient
            $result = $tcp.ConnectAsync($svc.Url -replace "http://", "").Split(":")[0], 9094 | Wait-Job -Timeout 5
            $tcp.Close()
            if ($result.State -eq "Completed") {
                Write-Host " OK" -ForegroundColor Green
                $healthy++
                $results += [PSCustomObject]@{
                    Name = $svc.Name
                    Status = "HEALTHY"
                    ResponseTime = "$($sw.ElapsedMilliseconds)ms"
                    Description = $svc.Description
                }
            } else {
                Write-Host " FAIL" -ForegroundColor Red
                $unhealthy++
                $results += [PSCustomObject]@{
                    Name = $svc.Name
                    Status = "UNHEALTHY"
                    ResponseTime = "timeout"
                    Description = $svc.Description
                }
            }
        } elseif ($svc.Type -eq "ping") {
            $resp = Invoke-WebRequest -Uri "$($svc.Url)$($svc.Endpoint)" -TimeoutSec 5 -UseBasicParsing
            if ($resp.StatusCode -eq 200 -and $resp.Content -eq "Ok") {
                Write-Host " OK" -ForegroundColor Green
                $healthy++
                $results += [PSCustomObject]@{
                    Name = $svc.Name
                    Status = "HEALTHY"
                    ResponseTime = "$($sw.ElapsedMilliseconds)ms"
                    Description = $svc.Description
                }
            } else {
                Write-Host " DEGRADED" -ForegroundColor Yellow
                $unhealthy++
                $results += [PSCustomObject]@{
                    Name = $svc.Name
                    Status = "DEGRADED"
                    ResponseTime = "$($sw.ElapsedMilliseconds)ms"
                    Description = $svc.Description
                }
            }
        } else {
            $resp = Invoke-WebRequest -Uri "$($svc.Url)$($svc.Endpoint)" -TimeoutSec 5 -UseBasicParsing
            if ($svc.Type -eq "json") {
                $content = $resp.Content | ConvertFrom-Json -ErrorAction SilentlyContinue
                if ($content -and ($content.PSObject.Properties.Name -match $svc.Expected)) {
                    Write-Host " OK" -ForegroundColor Green
                    $healthy++
                    $results += [PSCustomObject]@{
                        Name = $svc.Name
                        Status = "HEALTHY"
                        ResponseTime = "$($sw.ElapsedMilliseconds)ms"
                        Description = $svc.Description
                    }
                } else {
                    Write-Host " DEGRADED" -ForegroundColor Yellow
                    $unhealthy++
                    $results += [PSCustomObject]@{
                        Name = $svc.Name
                        Status = "DEGRADED"
                        ResponseTime = "$($sw.ElapsedMilliseconds)ms"
                        Description = $svc.Description
                    }
                }
            } else {
                Write-Host " OK" -ForegroundColor Green
                $healthy++
                $results += [PSCustomObject]@{
                    Name = $svc.Name
                    Status = "HEALTHY"
                    ResponseTime = "$($sw.ElapsedMilliseconds)ms"
                    Description = $svc.Description
                }
            }
        }
    } catch {
        Write-Host " FAIL" -ForegroundColor Red
        $unhealthy++
        $results += [PSCustomObject]@{
            Name = $svc.Name
            Status = "UNHEALTHY"
            ResponseTime = "error"
            Description = $svc.Description
        }
    }
    
    $sw.Stop()
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Summary: $healthy Healthy | $unhealthy Unhealthy" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

if ($unhealthy -eq 0) {
    Write-Host "All services are healthy!" -ForegroundColor Green
} else {
    Write-Host "Unhealthy services:" -ForegroundColor Red
    $results | Where-Object { $_.Status -eq "UNHEALTHY" } | Format-Table -AutoSize
    Write-Host ""
    Write-Host "Check Docker logs for errors:" -ForegroundColor Yellow
    Write-Host "  docker logs <container-name>" -ForegroundColor Cyan
}
