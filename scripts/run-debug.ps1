Write-Host "Starting Chronosched in DEBUG mode..." -ForegroundColor Yellow
docker compose -f docker-compose.yml -f docker-compose.debug.yml up --build
