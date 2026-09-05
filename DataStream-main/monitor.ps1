while ($true) {
    Clear-Host
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  LIVE MONITORING - $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    
    Write-Host "[1] CURRENT RATE:" -ForegroundColor Green
    $rate = curl.exe -s http://localhost:8080/api/generator/rate
    Write-Host "    $rate" -ForegroundColor White
    Write-Host ""
    
    Write-Host "[2] PUBLISHED (Prometheus):" -ForegroundColor Green
    $pub = curl.exe -s "http://localhost:9090/api/v1/query?query=orders_published_total"
    Write-Host "    $pub" -ForegroundColor White
    Write-Host ""
    
    Write-Host "[3] CLICKHOUSE ORDERS:" -ForegroundColor Green
    $ch = docker exec clickhouse clickhouse-client --query "SELECT count() FROM ecommerce.orders"
    Write-Host "    Total: $ch" -ForegroundColor White
    Write-Host ""
    
    Write-Host "[4] GO PRODUCER (last 3 lines):" -ForegroundColor Green
    docker logs go-producer --tail 3 2>$null | ForEach-Object { Write-Host "    $_" -ForegroundColor Gray }
    Write-Host ""
    
    Write-Host "[5] STREAM PROCESSOR (last 3 lines):" -ForegroundColor Green
    docker logs stream-processor --tail 3 2>$null | ForEach-Object { Write-Host "    $_" -ForegroundColor Gray }
    Write-Host ""
    
    Write-Host "[6] INTELLIGENCE PIPELINE (last 3 lines):" -ForegroundColor Green
    docker logs intelligence-pipeline --tail 3 2>$null | ForEach-Object { Write-Host "    $_" -ForegroundColor Gray }
    Write-Host ""
    
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "Press Ctrl+C to stop" -ForegroundColor Yellow
    Write-Host ""
    
    Start-Sleep -Seconds 5
}
