# NYC Taxi Lakehouse - Teardown Script
# =======================================
# This script safely tears down the lakehouse stack
# Usage: .\teardown.ps1 [-Environment dev|staging|prod] [-RemoveVolumes] [-Backup]

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("dev", "staging", "prod")]
    [string]$Environment,
    
    [Parameter(Mandatory=$false)]
    [switch]$RemoveVolumes,
    
    [Parameter(Mandatory=$false)]
    [switch]$Backup
)

# Configuration
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$BaseCompose = Join-Path $ProjectRoot "docker-compose.yaml"
$BackupDir = Join-Path $ProjectRoot "backups\pre-teardown"

# Colors
function Write-Info { Write-Host $args -ForegroundColor Cyan }
function Write-Success { Write-Host $args -ForegroundColor Green }
function Write-Warning { Write-Host $args -ForegroundColor Yellow }
function Write-Error { Write-Host $args -ForegroundColor Red }

# Banner
Write-Host "=" * 70 -ForegroundColor Yellow
Write-Host "NYC Taxi Lakehouse - Teardown Script" -ForegroundColor Yellow
Write-Host "=" * 70 -ForegroundColor Yellow
Write-Host ""

# Detect environment from .env if not specified
if (-not $Environment) {
    if (Test-Path ".env") {
        $envLine = Get-Content ".env" | Select-String "^ENVIRONMENT=" | Select-Object -First 1
        if ($envLine) {
            $Environment = ($envLine -split "=")[1].Trim()
            Write-Info "Detected environment from .env: $Environment"
        }
    }
}

if ($RemoveVolumes) {
    Write-Warning "WARNING: Volume removal requested!"
    Write-Warning "This will DELETE ALL DATA including:"
    Write-Warning "  - All Iceberg tables in MinIO"
    Write-Warning "  - All database data (PostgreSQL)"
    Write-Warning "  - All logs and checkpoints"
    Write-Host ""
    
    if ($Backup) {
        Write-Info "Backup flag is set. Data will be backed up first."
    } else {
        Write-Error "DATA WILL BE PERMANENTLY DELETED!"
    }
    
    Write-Host ""
    $confirmation = Read-Host "Type 'DELETE' to confirm volume removal"
    if ($confirmation -ne "DELETE") {
        Write-Info "Teardown cancelled."
        exit 0
    }
}

# Step 1: Backup if requested
if ($Backup) {
    Write-Info "Step 1: Creating backup before teardown"
    Write-Info "-----------------------------------------"
    
    $backupDate = Get-Date -Format "yyyyMMdd_HHmmss"
    $envTag = if ($Environment) { "$Environment-" } else { "" }
    $currentBackupDir = Join-Path $BackupDir "$envTag$backupDate"
    New-Item -ItemType Directory -Path $currentBackupDir -Force | Out-Null
    
    Write-Info "  Backing up to: $currentBackupDir"
    
    # Backup environment file
    if (Test-Path ".env") {
        Copy-Item ".env" "$currentBackupDir\env"
        Write-Success "  ✓ Environment file backed up"
    }
    
    # Backup databases
    Write-Info "  Backing up databases..."
    try {
        docker-compose -f $BaseCompose exec -T metastore-db pg_dump -U hive metastore > "$currentBackupDir\metastore.sql" 2>$null
        if (Test-Path "$currentBackupDir\metastore.sql") {
            Write-Success "  ✓ Metastore DB backed up"
        }
    } catch {
        Write-Warning "  ! Could not backup metastore DB"
    }
    
    try {
        docker-compose -f $BaseCompose exec -T superset-db pg_dump -U superset superset > "$currentBackupDir\superset.sql" 2>$null
        if (Test-Path "$currentBackupDir\superset.sql") {
            Write-Success "  ✓ Superset DB backed up"
        }
    } catch {
        Write-Warning "  ! Could not backup superset DB"
    }
    
    try {
        docker-compose -f $BaseCompose exec -T airflow-db pg_dump -U airflow airflow > "$currentBackupDir\airflow.sql" 2>$null
        if (Test-Path "$currentBackupDir\airflow.sql") {
            Write-Success "  ✓ Airflow DB backed up"
        }
    } catch {
        Write-Warning "  ! Could not backup airflow DB"
    }
    
    # Backup config
    if (Test-Path "config") {
        Copy-Item "config" "$currentBackupDir\config" -Recurse
        Write-Success "  ✓ Configuration backed up"
    }
    
    Write-Success "  Backup completed: $currentBackupDir"
    Write-Host ""
} else {
    Write-Warning "Step 1: Backup skipped"
    Write-Host ""
}

# Step 2: Stop containers
Write-Info "Step 2: Stopping containers"
Write-Info "----------------------------"

# Build compose args
$composeArgs = @("-f", $BaseCompose)
if ($Environment) {
    $OverrideCompose = Join-Path $ProjectRoot "docker-compose.override.$Environment.yaml"
    if (Test-Path $OverrideCompose) {
        $composeArgs += @("-f", $OverrideCompose)
    }
}

Write-Info "  Stopping all services gracefully..."
docker-compose @composeArgs stop
if ($LASTEXITCODE -eq 0) {
    Write-Success "✓ All services stopped"
} else {
    Write-Warning "! Some services may not have stopped cleanly"
}
Write-Host ""

# Step 3: Remove containers
Write-Info "Step 3: Removing containers"
Write-Info "----------------------------"

$removeArgs = $composeArgs + @("down")
if ($RemoveVolumes) {
    $removeArgs += "-v"
    Write-Warning "  Removing containers AND volumes..."
} else {
    Write-Info "  Removing containers (preserving volumes)..."
}

docker-compose @removeArgs
if ($LASTEXITCODE -eq 0) {
    Write-Success "✓ Containers removed"
    if ($RemoveVolumes) {
        Write-Success "✓ Volumes removed"
    }
} else {
    Write-Error "✗ Failed to remove containers"
    exit 1
}
Write-Host ""

# Step 4: Clean up network (if needed)
Write-Info "Step 4: Network cleanup"
Write-Info "------------------------"

# Check if lakehouse network exists
$network = docker network ls --filter name=lakehouse-net --format "{{.Name}}"
if ($network) {
    try {
        docker network rm lakehouse-net 2>$null
        Write-Success "✓ Network removed"
    } catch {
        Write-Info "  Network still in use or already removed"
    }
} else {
    Write-Info "  No lakehouse network found"
}
Write-Host ""

# Step 5: Orphan cleanup
Write-Info "Step 5: Removing orphan resources"
Write-Info "-----------------------------------"

# Remove orphaned volumes
if ($RemoveVolumes) {
    Write-Info "  Checking for orphaned volumes..."
    $orphanedVolumes = docker volume ls -q --filter "dangling=true"
    if ($orphanedVolumes) {
        docker volume rm $orphanedVolumes 2>$null
        Write-Success "  ✓ Orphaned volumes removed"
    } else {
        Write-Info "  No orphaned volumes found"
    }
}

# Remove orphaned networks
$orphanedNetworks = docker network ls -q --filter "dangling=true"
if ($orphanedNetworks) {
    docker network rm $orphanedNetworks 2>$null
    Write-Success "  ✓ Orphaned networks removed"
} else {
    Write-Info "  No orphaned networks found"
}

Write-Host ""

# Step 6: Verify cleanup
Write-Info "Step 6: Verification"
Write-Info "---------------------"

$remainingContainers = docker ps -a --filter "name=lakehouse" --format "{{.Names}}"
if ($remainingContainers) {
    Write-Warning "! Some lakehouse containers still exist:"
    $remainingContainers | ForEach-Object { Write-Warning "  - $_" }
} else {
    Write-Success "✓ No lakehouse containers found"
}

if (-not $RemoveVolumes) {
    $volumes = docker volume ls --filter "name=lakehouse" --format "{{.Name}}"
    if ($volumes) {
        Write-Info "Preserved volumes:"
        $volumes | ForEach-Object { Write-Info "  - $_" }
        Write-Info ""
        Write-Info "To remove volumes later: docker volume rm [volume-name]"
    }
}

Write-Host ""

# Completion
Write-Host "=" * 70 -ForegroundColor Green
Write-Success "Teardown Complete!"
Write-Host "=" * 70 -ForegroundColor Green
Write-Host ""

if ($Backup) {
    Write-Info "Backup location: $currentBackupDir"
    Write-Host ""
}

if ($RemoveVolumes) {
    Write-Warning "All data has been removed."
    if (-not $Backup) {
        Write-Warning "No backup was created. Data cannot be recovered."
    }
} else {
    Write-Info "Data volumes preserved. To restart:"
    Write-Info "  .\scripts\deploy.ps1 -Environment $Environment"
}

Write-Host ""
