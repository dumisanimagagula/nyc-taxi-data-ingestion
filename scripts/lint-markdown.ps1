# Markdown Linting Script
# =========================
# This script fixes common markdown linting errors:
# - MD040: Missing language identifiers in code blocks
# - MD022: Missing blank lines around headings
# - MD024: Duplicate heading content
# Usage: .\lint-markdown.ps1 [-Fix] [-DryRun]

param(
    [Parameter(Mandatory = $false)]
    [switch]$Fix,
    
    [Parameter(Mandatory = $false)]
    [switch]$DryRun,
    
    [Parameter(Mandatory = $false)]
    [string]$Path = "."
)

# Colors
function Write-Info { Write-Host $args -ForegroundColor Cyan }
function Write-Success { Write-Host $args -ForegroundColor Green }
function Write-Warning { Write-Host $args -ForegroundColor Yellow }
function Write-Error { Write-Host $args -ForegroundColor Red }

# Statistics
$script:stats = @{
    FilesScanned    = 0
    FilesWithIssues = 0
    MD040Fixed      = 0
    MD022Fixed      = 0
    MD024Fixed      = 0
    TODOsFound      = 0
}

# Find all markdown files
$markdownFiles = Get-ChildItem -Path $Path -Filter "*.md" -Recurse | Where-Object {
    $_.FullName -notmatch "\\node_modules\\" -and
    $_.FullName -notmatch "\\.git\\"
}

Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host "Markdown Linting Tool" -ForegroundColor Cyan
Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host ""

if ($DryRun) {
    Write-Warning "DRY RUN MODE - No changes will be made"
    Write-Host ""
}

foreach ($file in $markdownFiles) {
    $script:stats.FilesScanned++
    $relativePath = $file.FullName.Replace((Get-Location).Path, "").TrimStart('\')
    
    $content = Get-Content $file.FullName -Raw
    $originalContent = $content
    $issues = @()
    
    # MD040: Check for code blocks without language
    $codeBlocksWithoutLang = [regex]::Matches($content, '(?m)^```\s*$')
    if ($codeBlocksWithoutLang.Count -gt 0) {
        $issues += "MD040: $($codeBlocksWithoutLang.Count) code block(s) without language identifier"
        
        if ($Fix -and -not $DryRun) {
            # Try to infer language from context
            $lines = $content -split "`n"
            $newLines = @()
            $inCodeBlock = $false
            
            for ($i = 0; $i -lt $lines.Count; $i++) {
                $line = $lines[$i]
                
                if ($line -match '^```\s*$') {
                    if (-not $inCodeBlock) {
                        # Opening code block without language
                        # Try to infer from content
                        $nextLine = if ($i + 1 -lt $lines.Count) { $lines[$i + 1] } else { "" }
                        
                        $language = "text"
                        if ($nextLine -match '^\s*(import|from|def|class)\s') { $language = "python" }
                        elseif ($nextLine -match '^\s*(docker-compose|version:|services:)') { $language = "yaml" }
                        elseif ($nextLine -match '^\s*(function|const|let|var)\s') { $language = "javascript" }
                        elseif ($nextLine -match '^\s*(\$|Get-|Write-|Set-)') { $language = "powershell" }
                        elseif ($nextLine -match '^\s*(SELECT|INSERT|UPDATE|DELETE)\s') { $language = "sql" }
                        elseif ($nextLine -match '^\s*{') { $language = "json" }
                        elseif ($nextLine -match '^\s*export |^\s*cd |^\s*ls ') { $language = "bash" }
                        
                        $newLines += "``````$language"
                        $script:stats.MD040Fixed++
                        $inCodeBlock = $true
                    }
                    else {
                        # Closing code block
                        $newLines += $line
                        $inCodeBlock = $false
                    }
                }
                else {
                    $newLines += $line
                }
            }
            
            $content = $newLines -join "`n"
        }
    }
    
    # MD022: Check for missing blank lines around headings
    $headingIssues = [regex]::Matches($content, '(?m)^[^\n\r#].*\r?\n(#{1,6}\s+.+)')
    $headingIssues += [regex]::Matches($content, '(?m)(#{1,6}\s+.+)\r?\n[^\n\r#]')
    
    if ($headingIssues.Count -gt 0) {
        $issues += "MD022: $($headingIssues.Count) heading(s) without proper spacing"
        
        if ($Fix -and -not $DryRun) {
            # Add blank lines before/after headings
            $content = [regex]::Replace($content, '(?m)([^\n\r])\r?\n(#{1,6}\s+)', '$1' + "`n`n" + '$2')
            $content = [regex]::Replace($content, '(?m)(#{1,6}\s+.+)\r?\n([^\n\r#])', '$1' + "`n`n" + '$2')
            $script:stats.MD022Fixed++
        }
    }
    
    # MD024: Check for duplicate headings (informational only - requires manual review)
    $headings = [regex]::Matches($content, '(?m)^#{1,6}\s+(.+)$')
    $headingText = @{}
    foreach ($heading in $headings) {
        $text = $heading.Groups[1].Value.Trim()
        if ($headingText.ContainsKey($text)) {
            $headingText[$text]++
        }
        else {
            $headingText[$text] = 1
        }
    }
    
    $duplicates = $headingText.GetEnumerator() | Where-Object { $_.Value -gt 1 }
    if ($duplicates) {
        $dupCount = ($duplicates | Measure-Object).Count
        $issues += "MD024: $dupCount duplicate heading(s) found - manual review needed"
        $script:stats.MD024Fixed += $dupCount
    }
    
    # Check for TODOs/Placeholders
    $todos = [regex]::Matches($content, '(?i)(TODO|FIXME|XXX|\[TBD\]|\[TODO\]|placeholder)')
    if ($todos.Count -gt 0) {
        $issues += "INFO: $($todos.Count) TODO/placeholder(s) found"
        $script:stats.TODOsFound += $todos.Count
    }
    
    # Report issues
    if ($issues.Count -gt 0) {
        $script:stats.FilesWithIssues++
        Write-Warning "File: $relativePath"
        foreach ($issue in $issues) {
            Write-Host "  - $issue" -ForegroundColor Yellow
        }
        
        # Show duplicate headings for manual review
        if ($duplicates) {
            foreach ($dup in $duplicates) {
                Write-Host "    Duplicate: '$($dup.Key)' appears $($dup.Value) times" -ForegroundColor Gray
            }
        }
        
        Write-Host ""
    }
    
    # Write changes if Fix is enabled
    if ($Fix -and -not $DryRun -and $content -ne $originalContent) {
        Set-Content -Path $file.FullName -Value $content -NoNewline
        Write-Success "  Fixed: $relativePath"
    }
}

# Summary Report
Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host "Linting Summary" -ForegroundColor Cyan
Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host ""
Write-Host "Files scanned:        $($script:stats.FilesScanned)"
Write-Host "Files with issues:    $($script:stats.FilesWithIssues)"
Write-Host ""
Write-Host "MD040 (code lang):    $($script:stats.MD040Fixed) issue(s)" -ForegroundColor $(if ($script:stats.MD040Fixed -gt 0) { "Yellow" } else { "Green" })
Write-Host "MD022 (heading space): $($script:stats.MD022Fixed) issue(s)" -ForegroundColor $(if ($script:stats.MD022Fixed -gt 0) { "Yellow" } else { "Green" })
Write-Host "MD024 (dup headings): $($script:stats.MD024Fixed) issue(s)" -ForegroundColor $(if ($script:stats.MD024Fixed -gt 0) { "Yellow" } else { "Green" })
Write-Host "TODOs/Placeholders:   $($script:stats.TODOsFound)" -ForegroundColor $(if ($script:stats.TODOsFound -gt 0) { "Yellow" } else { "Green" })
Write-Host ""

if ($DryRun) {
    Write-Info "DRY RUN - No changes were made"
    Write-Info "Run with -Fix to apply changes"
}
elseif ($Fix) {
    Write-Success "Fixes applied!"
    Write-Info "Note: MD024 (duplicate headings) require manual review and fixing"
    Write-Info "Note: TODOs/Placeholders require manual resolution"
}
else {
    Write-Info "Run with -Fix to apply automatic fixes"
    Write-Info "Run with -DryRun -Fix to preview changes without applying"
}

Write-Host ""
Write-Host "=" * 80 -ForegroundColor Cyan
