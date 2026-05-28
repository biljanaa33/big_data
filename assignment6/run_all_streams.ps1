$ErrorActionPreference = "Stop"

$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = Join-Path $ProjectDir ".venv\Scripts\python.exe"

if (-not (Test-Path $Python)) {
    Write-Error "Could not find .venv Python at $Python. Create/activate the assignment virtual environment first."
}

function Start-StreamTerminal {
    param(
        [string] $Title,
        [string] $Command
    )

    $fullCommand = "cd /d `"$ProjectDir`" && title $Title && $Command"
    Start-Process "cmd.exe" -ArgumentList "/k", $fullCommand
}

Write-Host "Opening Kafka/streaming terminals from:"
Write-Host $ProjectDir
Write-Host ""

Start-StreamTerminal `
    -Title "Kafka Docker" `
    -Command "docker compose up -d"

Start-Sleep -Seconds 3

Start-StreamTerminal `
    -Title "Quix Borough Stats" `
    -Command "`"$Python`" quix_streams.py borough"

Start-StreamTerminal `
    -Title "Quix Location Stats" `
    -Command "`"$Python`" quix_streams.py location"

Start-StreamTerminal `
    -Title "Regular Python Stats" `
    -Command "`"$Python`" regular_python_stats.py"

Start-StreamTerminal `
    -Title "Stream Clustering" `
    -Command "`"$Python`" stream_clustering.py"

Start-Sleep -Seconds 5

Start-StreamTerminal `
    -Title "Taxi Producer" `
    -Command "`"$Python`" producer.py"

Write-Host "Started terminals:"
Write-Host "1. Kafka Docker"
Write-Host "2. Quix Borough Stats"
Write-Host "3. Quix Location Stats"
Write-Host "4. Regular Python Stats"
Write-Host "5. Stream Clustering"
Write-Host "6. Taxi Producer"
Write-Host ""
Write-Host "Optional checks after messages are produced:"
Write-Host ".\.venv\Scripts\python.exe read_topic.py borough --limit 1 --seconds 60"
Write-Host ".\.venv\Scripts\python.exe read_topic.py location --limit 1 --seconds 60"
Write-Host ".\.venv\Scripts\python.exe read_topic.py py-borough --limit 1 --seconds 60"
Write-Host ".\.venv\Scripts\python.exe read_topic.py py-location --limit 1 --seconds 60"
Write-Host ".\.venv\Scripts\python.exe read_topic.py clusters --limit 1 --seconds 60"
