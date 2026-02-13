# NYC Taxi Lakehouse - Health Check Script
# ===========================================
# This script checks the health of all lakehouse services
# Usage: .\healthcheck.ps1

param(
    [Parameter(Mandatory=$false)]
    [switch]$Detailed,
    
    [Parameter(Mandatory=$false)]
    [switch]$Watch
)

# Configuration
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$BaseCompose = Join-Path $ProjectRoot "docker-compose.yaml"

# Colors
function Write-Info { Write-Host $args -ForegroundColor Cyan }
function Write-Success { Write-Host $args -ForegroundColor Green }
function Write-Warning { Write-Host $args -ForegroundColor Yellow }
function Write-Error { Write-Host $args -ForegroundColor Red }

function Check-Services {
    Clear-Host
    
    # Banner
    Write-Host "=" * 80 -ForegroundColor Cyan
    Write-Host "NYC Taxi Lakehouse - Health Check" -ForegroundColor Cyan
    Write-Host " $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Cyan
    Write-Host "=" * 80 -ForegroundColor Cyan
    Write-Host ""
    
    # Get container status
    $containers = docker-compose -f $BaseCompose ps --format json | ConvertFrom-Json
    
    if (-not $containers) {
        Write-Warning "No lakehouse containers found. Stack may not be running."
        Write-Info "Start the stack with: .\scripts\deploy.ps1 -Environment dev"
        return
    }
    
    # Summary statistics
    $total = ($containers | Measure-Object).Count
    $running = ($containers | Where-Object { $_.State -eq "running" } | Measure-Object).Count
    $healthy = ($containers | Where-Object { $_.Health -eq "healthy" } | Measure-Object).Count
    $unhealthy = ($containers | Where-Object { $_.Health -eq "unhealthy" } | Measure-Object).Count
    $starting = ($containers | Where-Object { $_.Health -eq "starting" } | Measure-Object).Count
    
    Write-Info "Summary"
    Write-Info "--------"
    Write-Host "Total Services:    " -NoNewline
    Write-Host $total -ForegroundColor White
    Write-Host "Running:           " -NoNewline
    if ($running -eq $total) {
        Write-Success $running
    } else {
        Write-Warning $running
    }
    Write-Host "Healthy:           " -NoNewline
    if ($healthy -eq $total) {
        Write-Success $healthy
    } elseif ($healthy -gt 0) {
        Write-Warning $healthy
    } else {
        Write-Error $healthy
    }
    if ($starting -gt 0) {
        Write-Host "Starting:          " -NoNewline
        Write-Warning $starting
    }
    if ($unhealthy -gt 0) {
        Write-Host "Unhealthy:         " -NoNewline
        Write-Error $unhealthy
    }
    Write-Host ""
    
    # Service details
    Write-Info "Service Status"
    Write-Info ("-" * 80)
    Write-Host ("{0,-25} {1,-12} {2,-12} {3}" -f "SERVICE", "STATE", "HEALTH", "PORTS")
    Write-Host ("-" * 80)
    
    foreach ($container in $containers | Sort-Object Name) {
        $name = $container.Name -replace "lakehouse-", ""
        $state = $container.State
        $health = if ($container.Health) { $container.Health } else { "N/A" }
        $ports = if ($container.Publishers) {
            ($container.Publishers | ForEach-Object { "$($_.PublishedPort):$($_.TargetPort)" }) -join ", "
        } else {
            "-"
        }
        
        # Color code based on status
        Write-Host ("{0,-25} " -f $name) -NoNewline
        
        # State
        switch ($state) {
            "running" { Write-Success ("{0,-12} " -f $state) -NoNewline }
            "exited" { Write-Error ("{0,-12} " -f $state) -NoNewline }
            default { Write-Warning ("{0,-12} " -f $state) -NoNewline }
        }
        
        # Health
        switch ($health) {
            "healthy" { Write-Success ("{0,-12} " -f $health) -NoNewline }
            "unhealthy" { Write-Error ("{0,-12} " -f $health) -NoNewline }
            "starting" { Write-Warning ("{0,-12} " -f $health) -NoNewline }
            default { Write-Host ("{0,-12} " -f $health) -NoNewline }
        }
        
        # Ports
        Write-Host $ports
    }
    
    Write-Host ""
    
    # Detailed information
    if ($Detailed) {
        Write-Info "Detailed Information"
        Write-Info ("-" * 80)
        Write-Host ""
        
        foreach ($container in $containers | Sort-Object Name) {
            $name = $container.Name
            $fullInfo = docker inspect $name | ConvertFrom-Json
            
            Write-Host "Service: " -NoNewline
            Write-Success $name
            Write-Host "  Image:      $($fullInfo.Config.Image)"
            Write-Host "  Created:    $($fullInfo.Created)"
            Write-Host "  Status:     $($fullInfo.State.Status)"
            
            if ($fullInfo.State.Health) {
                Write-Host "  Health:     $($fullInfo.State.Health.Status)"
                Write-Host "  Failures:   $($fullInfo.State.Health.FailingStreak)"
                
                if ($fullInfo.State.Health.Log) {
                    $lastCheck = $fullInfo.State.Health.Log[-1]
                    Write-Host "  Last Check: $($lastCheck.Start)"
                    if ($lastCheck.Output) {
                        Write-Host "  Output:     $($lastCheck.Output.Substring(0, [Math]::Min(100, $lastCheck.Output.Length)))..."
                    }
                }
            }
            
            # Resource usage
            $stats = docker stats $name --no-stream --format json | ConvertFrom-Json
            if ($stats) {
                Write-Host "  CPU:        $($stats.CPUPerc)"
                Write-Host "  Memory:     $($stats.MemUsage)"
            }
            
            Write-Host ""
        }
    }
    
    # Resource summary
    Write-Info "Resource Usage"
    Write-Info ("-" * 80)
    Write-Host ("{0,-25} {1,-15} {2,-20} {3}" -f "SERVICE", "CPU %", "MEMORY USAGE", "NET I/O")
    Write-Host ("-" * 80)
    
    $allStats = docker stats --no-stream --format json | ConvertFrom-Json | Where-Object { $_.Name -like "lakehouse-*" }
    foreach ($stat in $allStats | Sort-Object Name) {
        $name = $stat.Name -replace "lakehouse-", ""
        Write-Host ("{0,-25} {1,-15} {2,-20} {3}" -f $name, $stat.CPUPerc, $stat.MemUsage, $stat.NetIO)
    }
    
    Write-Host ""
    
    # System resources
    Write-Info "System Resources"
    Write-Info ("-" * 80)
    
    $dockerInfo = docker info --format json | ConvertFrom-Json
    Write-Host "Containers Running: $($dockerInfo.ContainersRunning)"
    Write-Host "CPUs:               $($dockerInfo.NCPU)"
    Write-Host "Total Memory:       $([math]::Round($dockerInfo.MemTotal / 1GB, 2)) GB"
    
    # Disk usage
    $dfOutput = docker system df --format json | ConvertFrom-Json
    foreach ($item in $dfOutput) {
        if ($item.Type -eq "Volumes") {
            Write-Host "Volume Storage:     $($item.Size)"
        }
    }
    
    Write-Host ""
    
    # Access URLs
    Write-Info "Access URLs"
    Write-Info ("-" * 80)
    Write-Success "MinIO Console:      http://localhost:9001"
    Write-Success "Spark Master UI:    http://localhost:8080"
    Write-Success "Trino UI:           http://localhost:8086"
    Write-Success "Superset:           http://localhost:8088"
    Write-Success "Airflow:            http://localhost:8089"
    Write-Host ""
    
    # Status summary
    Write-Info "Overall Status"
    Write-Info ("-" * 80)
    
    if ($healthy -eq $total) {
        Write-Success "✓ All services are healthy and running normally"
    } elseif ($starting -gt 0 -and $unhealthy -eq 0) {
        Write-Warning "! Some services are still starting up. Please wait..."
    } elseif ($unhealthy -gt 0) {
        Write-Error "✗ $unhealthy service(s) are unhealthy. Check logs with:"
        Write-Error "  docker-compose logs [service-name]"
    } else {
        Write-Warning "! Stack health status unclear. Some services may not have health checks."
    }
    
    Write-Host ""
    Write-Host "=" * 80 -ForegroundColor Cyan
    Write-Host ""
}

# Main logic
if ($Watch) {
    Write-Info "Starting watch mode. Press Ctrl+C to exit."
    Write-Host ""
    while ($true) {
        Check-Services
        Start-Sleep -Seconds 5
    }
} else {
    Check-Services
}

# Additional commands
Write-Info "Useful Commands"
Write-Info "----------------"
Write-Info "  View logs:        docker-compose logs -f [service-name]"
Write-Info "  Restart service:  docker-compose restart [service-name]"
Write-Info "  Stop stack:       docker-compose down"
Write-Info "  Watch mode:       .\scripts\healthcheck.ps1 -Watch"
Write-Info "  Detailed info:    .\scripts\healthcheck.ps1 -Detailed"
Write-Host ""
