# Enable strict mode
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Function to check if a command exists
function Test-CommandExists {
    param ($command)
    $exists = $null -ne (Get-Command $command -ErrorAction SilentlyContinue)
    return $exists
}

# Check if psql is installed
if (-not (Test-CommandExists "psql")) {
    Write-Error "Error: psql is not installed."
    exit 1
}

# Check if sqlx is installed
if (-not (Test-CommandExists "sqlx")) {
    Write-Error "Error: sqlx is not installed."
    Write-Host "Use:"
    Write-Host "    cargo install --version='~0.6' sqlx-cli --no-default-features --features rustls,postgres"
    Write-Host "to install it."
    exit 1
}

# Environment variables
$DB_USER = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "postgres" }
$DB_PASSWORD = if ($env:POSTGRES_PASSWORD) { $env:POSTGRES_PASSWORD } else { "password" }
$DB_NAME = if ($env:POSTGRES_DB) { $env:POSTGRES_DB } else { "exampledb" }
$DB_PORT = if ($env:POSTGRES_PORT) { $env:POSTGRES_PORT } else { "6543" }
$DB_HOST = if ($env:POSTGRES_HOST) { $env:POSTGRES_HOST } else { "localhost" }

# Launch postgres using Docker
if (-not $env:SKIP_DOCKER) {
    docker run -e POSTGRES_USER=$DB_USER -e POSTGRES_PASSWORD=$DB_PASSWORD -e POSTGRES_DB=$DB_NAME -p "${DB_PORT}:5432" -d postgres postgres -N 1000
}

# Keep pinging Postgres until it's ready to accept commands
$env:PGPASSWORD = $DB_PASSWORD

Start-Sleep -Seconds 2
# do {
#     Start-Sleep -Seconds 1
#     $connected = $null -ne (psql -h $DB_HOST -U $DB_USER -p $DB_PORT -d "postgres" -c '\q' )
#     if (-not $connected) {
#         Write-Host "Postgres is still unavailable - sleeping"
#     }
# } while (-not $connected)

Write-Host "Postgres is up and running on port $DB_PORT - running migration now!"

$env:DATABASE_URL = "postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
sqlx database create
sqlx migrate run

Write-Host "Postgres has been migrated, ready to go!"