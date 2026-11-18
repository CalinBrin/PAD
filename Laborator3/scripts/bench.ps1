# Measurements for docs/evidence/results (same scenarios as bench.sh):
# with / without cache, healthy / slow replica / replica down / Redis down,
# round robin / least connections. The load generator runs inside the compose
# network (tools service); the faults are injected from the host.
#
# Usage (Windows):  powershell -ExecutionPolicy Bypass -File scripts\bench.ps1 [-Duration 20s] [-Concurrency 16]
#       (pwsh):     pwsh scripts/bench.ps1
# The system must be started first: docker compose up --build -d
param(
    [string]$Duration = '20s',
    [int]$Concurrency = 16,
    [string]$Admin = 'http://127.0.0.1:8081',
    [string[]]$DwAdmin = @('http://127.0.0.1:9101', 'http://127.0.0.1:9102', 'http://127.0.0.1:9103')
)
. (Join-Path $PSScriptRoot 'lib.ps1')
# also accept "url1,url2,url3" (pwsh -File passes a list as one string)
$script:DwAdmin = @($DwAdmin | ForEach-Object { $_ -split ',' } | Where-Object { $_ })
$OUT = 'docs/evidence/results'

function Set-Cache([string]$v) { & $script:Curl -fsS -X POST "$Admin/proxy/cache?enabled=$v" | Out-Null }
function Set-Algo([string]$v) { & $script:Curl -fsS -X POST "$Admin/proxy/balancer?algorithm=$v" | Out-Null }
function Reset-All { Reset-Faults; Set-Cache true; Set-Algo round_robin }
function Wait-Healthy {
    for ($i = 0; $i -lt 60; $i++) {
        $n = ([regex]::Matches((Http GET "$Admin/proxy/stats").Body, '"healthy": true')).Count
        if ($n -ge 3) { return }
        Start-Sleep -Seconds 1
    }
    throw 'replicas not healthy'
}
function Run([string]$Label, [string]$Scenario, [string[]]$Extra = @()) {
    $a = @('compose', 'run', '--rm', '-T', 'tools', 'go', 'run', './src/loadgen', '-url', 'http://proxy:8080',
        '-admin', 'http://proxy:8081', '-duration', $Duration, '-c', "$Concurrency", '-ids', '50',
        '-write-ratio', '0.02', '-dist', 'zipf', '-out', $OUT, '-label', $Label, '-scenario', $Scenario) + $Extra
    & docker @a 2>$null | Where-Object { $_ -match '^\d\d-' }
}

New-Item -ItemType Directory -Force -Path $OUT | Out-Null
Get-ChildItem $OUT -Include *.json, summary.csv, summary.md -Recurse | Remove-Item
Reset-All; Wait-Healthy
& docker compose run --rm -T tools go run ./src/loadgen -url http://proxy:8080 '-admin=' -duration 1s -c 1 -seed -label warmup -out /tmp 2>$null | Out-Null

Run '01-healthy-cache-on' '3 replici sanatoase'
Set-Cache false
Run '02-healthy-cache-off' '3 replici sanatoase'

Reset-All; Fault 2 'latency=200ms'
Run '03-slow-cache-on' 'dw-2 lent (+200 ms)'
Set-Cache false
Run '04-slow-cache-off' 'dw-2 lent (+200 ms)'
Set-Algo least_conn
Run '05-slow-cache-off-lc' 'dw-2 lent (+200 ms), least_conn'

Reset-All
Docker-Quiet compose stop dw-2
Run '06-down-cache-on' 'dw-2 oprit (docker stop) la inceputul rularii'
Set-Cache false
Run '07-down-cache-off' 'dw-2 oprit (docker stop)'
Docker-Quiet compose start dw-2
Reset-All; Wait-Healthy

Docker-Quiet compose stop redis
Run '08-redis-down' 'Redis oprit: proxy fara cache (degradat)'
Docker-Quiet compose start redis
Start-Sleep -Seconds 3
Reset-All
Run '09-healthy-cache-on-uniform' '3 replici sanatoase, 500 de angajati, acces uniform' @('-ids', '500', '-dist', 'uniform', '-seed')

& docker compose run --rm -T tools go run ./src/loadgen -summarize $OUT 2>$null | Out-Null
''
Get-Content "$OUT/summary.md" -Encoding UTF8
