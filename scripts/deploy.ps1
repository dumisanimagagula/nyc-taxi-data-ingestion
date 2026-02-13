# NYC Taxi Lakehouse - Deployment Script
# =========================================
# This script deploys the lakehouse stack for a specified environment
# Usage: .\deploy.ps1 -Environment dev|staging|prod

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("dev", "staging", "prod")]
    [string]$Environment,
    
    [Parameter(Mandatory = $false)]
    [switch]$SkipBackup,
    
    [Parameter(Mandatory = $false)]
    [switch]$SkipHealthCheck,
    
    [Parameter(Mandatory = $false)]
    [switch]$Detached = $true
)

# Configuration
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$EnvFile = Join-Path $ProjectRoot ".env.$Environment"
$BaseCompose = Join-Path $ProjectRoot "docker-compose.yaml"
$OverrideCompose = Join-Path $ProjectRoot "docker-compose.override.$Environment.yaml"
$BackupDir = Join-Path $ProjectRoot "backups\pre-deployment"

# Colors for output
function Write-Info { Write-Host $args -ForegroundColor Cyan }
function Write-Success { Write-Host $args -ForegroundColor Green }
function Write-Warning { Write-Host $args -ForegroundColor Yellow }
function Write-Error { Write-Host $args -ForegroundColor Red }

# Banner
Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host "NYC Taxi Lakehouse - Deployment Script" -ForegroundColor Cyan
Write-Host "Environment: $Environment" -ForegroundColor Cyan
Write-Host "=" * 70 -ForegroundColor Cyan
Write-Host ""

# Step 1: Pre-flight checks
Write-Info "Step 1: Pre-flight checks"
Write-Info "----------------------------"

# Check Docker is running
try {
    docker info | Out-Null
    Write-Success "✓ Docker is running"
}
catch {
    Write-Error "✗ Docker is not running. Please start Docker Desktop."
    exit 1
}

# Check Docker Compose version
$composeVersion = docker-compose version --short
if ($composeVersion -lt "2.0") {
    Write-Warning "! Docker Compose version $composeVersion detected. Version 2.0+ recommended."
}
Write-Success "✓ Docker Compose version: $composeVersion"

# Check environment file exists
if (-not (Test-Path $EnvFile)) {
    Write-Error "✗ Environment file not found: $EnvFile"
    Write-Info "  Please copy .env.$Environment.example or create the file."
    exit 1
}
Write-Success "✓ Environment file found: $EnvFile"

# Check compose files exist
if (-not (Test-Path $BaseCompose)) {
    Write-Error "✗ Base compose file not found: $BaseCompose"
    exit 1
}
Write-Success "✓ Base compose file found"

if (-not (Test-Path $OverrideCompose)) {
    Write-Warning "! Override file not found: $OverrideCompose"
    Write-Info "  Proceeding with base compose only"
    $OverrideCompose = $null
}

# Check for CHANGE_THIS in production
if ($Environment -eq "prod") {
    $changeThisCount = (Get-Content $EnvFile | Select-String "CHANGE_THIS" | Measure-Object).Count
    if ($changeThisCount -gt 0) {
        Write-Error "✗ Production .env file contains $changeThisCount 'CHANGE_THIS' placeholders"
        Write-Error "  All passwords must be changed before production deployment!"
        exit 1
    }
    Write-Success "✓ No password placeholders found in production .env"
}

# Check available disk space
$drive = (Get-Location).Drive.Name
$disk = Get-PSDrive $drive
$freeSpaceGB = [math]::Round($disk.Free / 1GB, 2)
$requiredSpaceGB = switch ($Environment) {
    "dev" { 50 }
    "staging" { 100 }
    "prod" { 500 }
}

if ($freeSpaceGB -lt $requiredSpaceGB) {
    Write-Warning "! Low disk space: ${freeSpaceGB}GB free. Recommended: ${requiredSpaceGB}GB"
}
else {
    Write-Success "✓ Sufficient disk space: ${freeSpaceGB}GB free"
}

# Check port availability
$requiredPorts = @(9000, 9001, 8080, 8086, 8088, 8089, 9083)
$portsInUse = @()
foreach ($port in $requiredPorts) {
    $connection = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue
    if ($connection) {
        $portsInUse += $port
    }
}

if ($portsInUse.Count -gt 0) {
    Write-Warning "! Ports already in use: $($portsInUse -join ', ')"
    Write-Info "  These ports may conflict with the lakehouse services."
    $continue = Read-Host "Continue anyway? (y/N)"
    if ($continue -ne "y") {
        exit 1
    }
}
else {
    Write-Success "✓ All required ports available"
}

Write-Host ""

# Step 2: Backup (if not skipped and stack exists)
if (-not $SkipBackup) {
    Write-Info "Step 2: Creating backup"
    Write-Info "------------------------"
    
    # Check if any containers are running
    $runningContainers = docker-compose -f $BaseCompose ps -q | Measure-Object
    
    if ($runningContainers.Count -gt 0) {
        $backupDate = Get-Date -Format "yyyyMMdd_HHmmss"
        $currentBackupDir = Join-Path $BackupDir "$Environment-$backupDate"
        New-Item -ItemType Directory -Path $currentBackupDir -Force | Out-Null
        
        Write-Info "  Backing up to: $currentBackupDir"
        
        # Backup environment file
        Copy-Item ".env" "$currentBackupDir\env" -ErrorAction SilentlyContinue
        Write-Success "  ✓ Environment file backed up"
        
        # Backup databases
        try {
            docker-compose -f $BaseCompose exec -T metastore-db pg_dump -U hive metastore > "$currentBackupDir\metastore.sql"
            Write-Success "  ✓ Metastore DB backed up"
        }
        catch {
            Write-Warning "  ! Could not backup metastore DB (may not be running)"
        }
        
        try {
            docker-compose -f $BaseCompose exec -T superset-db pg_dump -U superset superset > "$currentBackupDir\superset.sql"
            Write-Success "  ✓ Superset DB backed up"
        }
        catch {
            Write-Warning "  ! Could not backup superset DB (may not be running)"
        }
        
        try {
            docker-compose -f $BaseCompose exec -T airflow-db pg_dump -U airflow airflow > "$currentBackupDir\airflow.sql"
            Write-Success "  ✓ Airflow DB backed up"
        }
        catch {
            Write-Warning "  ! Could not backup airflow DB (may not be running)"
        }
        
        Write-Success "  Backup completed: $currentBackupDir"
    }
    else {
        Write-Info "  No running stack detected, skipping backup"
    }
    Write-Host ""
}
else {
    Write-Warning "Step 2: Backup skipped (--SkipBackup flag used)"
    Write-Host ""
}

# Step 3: Copy environment file
Write-Info "Step 3: Setting up environment"
Write-Info "--------------------------------"
Copy-Item $EnvFile ".env" -Force
Write-Success "✓ Copied $EnvFile to .env"
Write-Host ""

# Step 4: Pull images
Write-Info "Step 4: Pulling Docker images"
Write-Info "------------------------------"
$pullArgs = @("-f", $BaseCompose)
if ($OverrideCompose) {
    $pullArgs += @("-f", $OverrideCompose)
}
$pullArgs += "pull"

docker-compose @pullArgs
if ($LASTEXITCODE -ne 0) {
    Write-Error "✗ Failed to pull images"
    exit 1
}
Write-Success "✓ Images pulled successfully"
Write-Host ""

# Step 5: Build custom images
Write-Info "Step 5: Building custom images"
Write-Info "-------------------------------"
$buildArgs = @("-f", $BaseCompose)
if ($OverrideCompose) {
    $buildArgs += @("-f", $OverrideCompose)
}
$buildArgs += "build"

docker-compose @buildArgs
if ($LASTEXITCODE -ne 0) {
    Write-Error "✗ Failed to build images"
    exit 1
}
Write-Success "✓ Custom images built successfully"
Write-Host ""

# Step 6: Start stack
Write-Info "Step 6: Starting stack"
Write-Info "-----------------------"
$upArgs = @("-f", $BaseCompose)
if ($OverrideCompose) {
    $upArgs += @("-f", $OverrideCompose)
}
$upArgs += "up"
if ($Detached) {
    $upArgs += "-d"
}

Write-Info "  Command: docker-compose $($upArgs -join ' ')"
docker-compose @upArgs
if ($LASTEXITCODE -ne 0) {
    Write-Error "✗ Failed to start stack"
    exit 1
}
Write-Success "✓ Stack started successfully"
Write-Host ""

# Step 7: Health checks (if not skipped)
if (-not $SkipHealthCheck -and $Detached) {
    Write-Info "Step 7: Waiting for services to be healthy"
    Write-Info "--------------------------------------------"
    
    $maxWaitMinutes = switch ($Environment) {
        "dev" { 5 }
        "staging" { 7 }
        "prod" { 10 }
    }
    $maxWaitSeconds = $maxWaitMinutes * 60
    $checkInterval = 10
    $elapsed = 0
    
    Write-Info "  Maximum wait time: $maxWaitMinutes minutes"
    Write-Info "  Checking every $checkInterval seconds..."
    Write-Host ""
    
    while ($elapsed -lt $maxWaitSeconds) {
        Start-Sleep -Seconds $checkInterval
        $elapsed += $checkInterval
        
        # Get service status
        $services = docker-compose -f $BaseCompose ps --format json | ConvertFrom-Json
        $totalServices = ($services | Measure-Object).Count
        $healthyServices = ($services | Where-Object { $_.Health -eq "healthy" } | Measure-Object).Count
        $unhealthyServices = ($services | Where-Object { $_.Health -eq "unhealthy" } | Measure-Object).Count
        
        # Progress indicator
        $percent = if ($totalServices -gt 0) { [math]::Round(($healthyServices / $totalServices) * 100, 0) } else { 0 }
        $progressBar = "[" + ("=" * [math]::Floor($percent / 2)) + (" " * (50 - [math]::Floor($percent / 2))) + "]"
        
        Write-Host "`r  $progressBar $percent% ($healthyServices/$totalServices healthy)" -NoNewline
        
        # Check if all healthy
        if ($healthyServices -eq $totalServices) {
            Write-Host ""
            Write-Success "`n  ✓ All services are healthy!"
            break
        }
        
        # Check for unhealthy services
        if ($unhealthyServices -gt 0) {
            Write-Host ""
            Write-Warning "`n  ! $unhealthyServices service(s) reporting unhealthy"
        }
    }
    
    if ($elapsed -ge $maxWaitSeconds) {
        Write-Host ""
        Write-Warning "  ! Health check timeout reached ($maxWaitMinutes minutes)"
        Write-Info "  Some services may still be starting up. Check with: docker-compose ps"
    }
    
    Write-Host ""
}
else {
    Write-Warning "Step 7: Health check skipped"
    Write-Host ""
}

# Step 8: Display service status
Write-Info "Step 8: Service Status"
Write-Info "-----------------------"
docker-compose -f $BaseCompose ps
Write-Host ""

# Step 9: Access URLs
Write-Info "Step 9: Access Information"
Write-Info "---------------------------"
Write-Success "MinIO Console:  http://localhost:9001"
Write-Success "Spark Master:   http://localhost:8080"
Write-Success "Trino UI:       http://localhost:8086"
Write-Success "Superset:       http://localhost:8088"
Write-Success "Airflow:        http://localhost:8089"
Write-Host ""

# Read credentials from .env
$envContent = Get-Content ".env" | Where-Object { $_ -notmatch "^#" -and $_ -match "=" }
$envVars = @{}
foreach ($line in $envContent) {
    $parts = $line -split "=", 2
    if ($parts.Length -eq 2) {
        $envVars[$parts[0].Trim()] = $parts[1].Trim()
    }
}

Write-Info "Default Credentials:"
Write-Info "  MinIO:    $($envVars['MINIO_ROOT_USER']) / $($envVars['MINIO_ROOT_PASSWORD'])"
Write-Info "  Superset: $($envVars['SUPERSET_ADMIN_USERNAME']) / $($envVars['SUPERSET_ADMIN_PASSWORD'])"
Write-Info "  Airflow:  $($envVars['AIRFLOW_ADMIN_USERNAME']) / $($envVars['AIRFLOW_ADMIN_PASSWORD'])"
Write-Host ""

# Completion
Write-Host "=" * 70 -ForegroundColor Green
Write-Success "Deployment Complete!"
Write-Host "=" * 70 -ForegroundColor Green
Write-Host ""
Write-Info "Next steps:"
Write-Info "  1. Verify all services: docker-compose ps"
Write-Info "  2. Check logs: docker-compose logs -f"
Write-Info "  3. Access UIs using the URLs above"
Write-Info "  4. Run validation tests (if available)"
Write-Host ""
Write-Info "To stop: docker-compose down"
Write-Info "To view logs: docker-compose logs -f [service-name]"
Write-Host ""
