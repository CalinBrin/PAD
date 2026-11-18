# Reproduces every failure scenario on the running system and writes the
# evidence (client answers + the logs of every component, filtered by
# correlation ID) to docs/evidence/demo-*.log. Same scenarios as demo.sh.
#
# Usage (Windows):  powershell -ExecutionPolicy Bypass -File scripts\demo.ps1
#       (pwsh):     pwsh scripts/demo.ps1
# The system must be started first: docker compose up --build -d
param(
    [string]$Proxy = 'http://localhost:8080',
    [string]$Admin = 'http://127.0.0.1:8081',
    [string[]]$DwAdmin = @('http://127.0.0.1:9101', 'http://127.0.0.1:9102', 'http://127.0.0.1:9103')
)
. (Join-Path $PSScriptRoot 'lib.ps1')
$script:Proxy = $Proxy
# also accept "url1,url2,url3" (pwsh -File passes a list as one string)
$script:DwAdmin = @($DwAdmin | ForEach-Object { $_ -split ',' } | Where-Object { $_ })
$EV = 'docs/evidence'
$ID = 'emp-j-005'
New-Item -ItemType Directory -Force -Path $EV | Out-Null

function Emp($salary) { '{"id":"' + $ID + '","name":"Demo Employee","department":"IT","position":"Engineer","email":"demo@example.md","salary":' + $salary + '}' }
function Header($title, $detail) { @("# $title", "# $detail", ('# generat de scripts/demo.ps1 la ' + (Now-Iso)), '') }
function Save($file, $lines, $from, $to) {
    Write-Utf8 $file $lines
    # print the client part
    $show = $false
    foreach ($l in $lines) {
        if ($l -like "$from*") { $show = $true }
        if ($to -and $l -like "$to*") { break }
        if ($show) { $l }
    }
}
function Backend-State($name) {
    $s = Http GET ($Admin + '/proxy/stats')
    [regex]::Match(($s.Body -replace '\s', ''), '"name":"' + $name + '","url":"[^"]*","healthy":(true|false)').Value
}

Reset-Faults
& $script:Curl -fsS -X POST ($Admin + '/proxy/cache?enabled=true') | Out-Null

# ---------------------------------------------------------------------------
'1. cache: MISS, HIT, invalidare la PUT'
$S = Now-Iso
$out = Header 'Cache: MISS, HIT, JSON/XML separat, invalidare per angajat la PUT' 'correlation_id=demo-cache-001-*'
$out += '## Raspunsuri (client)'
$out += Show 'demo-cache-001-1' PUT "/employees/$ID" (Emp 30000)
$out += Show 'demo-cache-001-2' GET "/employees/$ID"
$out += Show 'demo-cache-001-3' GET "/employees/$ID"
$out += Show 'demo-cache-001-4' GET "/employees/$ID`?format=xml"
$out += Show 'demo-cache-001-5' GET '/employees/emp-j-006'
$out += Show 'demo-cache-001-6' PUT "/employees/$ID" (Emp 31000)
$out += Show 'demo-cache-001-7' GET "/employees/$ID"
$out += Show 'demo-cache-001-8' GET "/employees/$ID"
$out += Show 'demo-cache-001-9' GET '/employees/emp-j-006'
Start-Sleep -Seconds 1
$out += @('', '## Loguri (proxy, replici)')
$out += Logs $S 'demo-cache-001'
Save "$EV/demo-cache-001.log" $out '## Raspunsuri' '## Loguri'

# ---------------------------------------------------------------------------
'2. cursa GET lent / PUT (remarca 1)'
$S = Now-Iso
Show 'demo-race-001-0' GET "/employees/$ID" | Out-Null
1..3 | ForEach-Object { Fault $_ 'read_delay=1500ms' }
$out = Header 'Cursa: un GET inceput inainte de un PUT citeste valoarea veche si raspunde dupa invalidare' 'read_delay=1500ms pe toate replicile (dupa citire, inainte de raspuns); correlation_id=demo-race-001-*'
$out += '## Raspunsuri (client)'
# the slow GET runs in the background (no-cache: it must reach a replica, then fill the cache)
$tmp = [System.IO.Path]::GetTempFileName()
$argLine = '-s -i -H "Cache-Control: no-cache" -H "X-Correlation-ID: demo-race-001-1" ' + "$Proxy/employees/$ID"
$bg = Start-Process -FilePath $script:Curl -ArgumentList $argLine -RedirectStandardOutput $tmp -NoNewWindow -PassThru
Start-Sleep -Milliseconds 400
$out += Show 'demo-race-001-2' PUT "/employees/$ID" (Emp 32000)
$bg.WaitForExit()
$out += Format-Answer GET "/employees/$ID" (Parse-Curl (Get-Content $tmp))
Remove-Item $tmp -ErrorAction SilentlyContinue
$out += @('', '## Intrarea scrisa in Redis de GET-ul lent (generatiile citite INAINTE de PUT)')
$entry = docker compose exec -T redis redis-cli GET "cache:employees:GET:/employees/$ID`?:json"
$out += [regex]::Matches(($entry -join ''), '"gens":\[[^\]]*\]|"stored_at":"[^"]*"') | ForEach-Object { $_.Value }
$ga = (docker compose exec -T redis redis-cli GET gen:employees:all) -join ''
$gi = (docker compose exec -T redis redis-cli GET "gen:employees:item:$ID") -join ''
if (-not $ga) { $ga = '0' }; if (-not $gi) { $gi = '0' }
$out += "contoarele curente: [gen:employees:all, gen:employees:item:$ID] = [$ga,$gi]  => intrarea nu mai e valida, nu se serveste"
1..3 | ForEach-Object { Fault $_ 'reset=1' }
$out += ''
$out += Show 'demo-race-001-3' GET "/employees/$ID"
$out += Show 'demo-race-001-4' GET "/employees/$ID"
Start-Sleep -Seconds 1
$out += @('', '## Loguri')
$out += Logs $S 'demo-race-001'
Save "$EV/demo-race-001.log" $out '## Raspunsuri' '## Loguri'

# ---------------------------------------------------------------------------
'3. PUT expirat, dar aplicat (remarca 1)'
$S = Now-Iso
Show 'demo-put-timeout-001-0' GET "/employees/$ID" | Out-Null
1..3 | ForEach-Object { Fault $_ 'write_delay=3s' }
$out = Header 'PUT aplicat de replica, dar raspunsul intarzie peste UPSTREAM_TIMEOUT (2s)' 'write_delay=3s pe toate replicile (dupa commit, inainte de raspuns); correlation_id=demo-put-timeout-001-*'
$out += '## Raspunsuri (client)'
$out += Show 'demo-put-timeout-001-1' GET "/employees/$ID"
$out += Show 'demo-put-timeout-001-2' PUT "/employees/$ID" (Emp 33000)
1..3 | ForEach-Object { Fault $_ 'reset=1' }
$out += Show 'demo-put-timeout-001-3' GET "/employees/$ID"
$out += Show 'demo-put-timeout-001-4' GET "/employees/$ID"
Start-Sleep -Seconds 4   # WRITE_GRACE: the delayed invalidation
$out += @('', '## Loguri')
$out += Logs $S 'demo-put-timeout-001'
Save "$EV/demo-put-timeout-001.log" $out '## Raspunsuri' '## Loguri'

# ---------------------------------------------------------------------------
'4. modificare de la client, preluata de sursa prin pull'
$S = Now-Iso
$stamp = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$out = Header 'POST de la client (cu Idempotency-Key), apoi pull de la nodul sursa XML' 'correlation_id=demo-pull-001-*'
$out += '## Raspunsuri (client)'
$out += Show 'demo-pull-001-1' POST '/employees/emp-x-003' '{"salary":45000,"position":"Manager"}' @{ 'Idempotency-Key' = "demo-pull-key-$stamp" }
$out += '(acelasi POST, aceeasi cheie: reluat, fara efect nou)'
$out += Show 'demo-pull-001-2' POST '/employees/emp-x-003' '{"salary":46000}' @{ 'Idempotency-Key' = "demo-pull-replay-$stamp" }
$out += Show 'demo-pull-001-3' POST '/employees/emp-x-003' '{"salary":46000}' @{ 'Idempotency-Key' = "demo-pull-replay-$stamp" }
Start-Sleep -Seconds 5
$out += @('', '## Loguri (proxy, replici, source-xml)')
$out += Logs $S 'demo-pull-001|change_applied.*emp-x-003'
Save "$EV/demo-pull-001.log" $out '## Raspunsuri' '## Loguri'

# ---------------------------------------------------------------------------
'5. health check activ, fara trafic (remarca 3)'
$S = Now-Iso
$out = Header 'Health check activ: dw-3 oprit si repornit, fara nicio cerere de la clienti' 'HEALTH_INTERVAL=2s, HEALTH_FALL=2, HEALTH_RISE=2'
$out += '## Actiuni'
$out += ((Now-Iso) + ' docker compose stop dw-3'); Docker-Quiet compose stop dw-3
Start-Sleep -Seconds 6
$out += ((Now-Iso) + ' stare backend-uri: ' + (Backend-State 'dw-3'))
$out += ((Now-Iso) + ' docker compose start dw-3'); Docker-Quiet compose start dw-3
Start-Sleep -Seconds 8
$out += ((Now-Iso) + ' stare backend-uri: ' + (Backend-State 'dw-3'))
$out += @('', '## Loguri (proxy)')
$out += Logs $S 'backend_down|backend_up'
Save "$EV/demo-health-001.log" $out '## Actiuni' '## Loguri'

# ---------------------------------------------------------------------------
'6. circuit breaker intre doua health check-uri'
$S = Now-Iso
Fault 1 'fail=true'
$out = Header 'dw-1 raspunde 503 la cereri, dar /readyz e OK: detectie pasiva (breaker)' 'BREAKER_THRESHOLD=3, BREAKER_COOLDOWN=5s; Cache-Control: no-cache ca fiecare GET sa ajunga la o replica'
$out += '## Raspunsuri (client)'
foreach ($n in 1..9) { $out += Show "demo-breaker-001-$n" GET "/employees/$ID" '' @{ 'Cache-Control' = 'no-cache' } }
Fault 1 'fail=false'
$out += ((Now-Iso) + ' dw-1 reparat; astept cooldown-ul'); Start-Sleep -Seconds 6
foreach ($n in 10..13) { $out += Show "demo-breaker-001-$n" GET "/employees/$ID" '' @{ 'Cache-Control' = 'no-cache' } }
$out += @('', '## Loguri')
$out += Logs $S 'demo-breaker-001|breaker_'
Save "$EV/demo-breaker-001.log" $out '## Raspunsuri' '## Loguri'

# ---------------------------------------------------------------------------
'7. Redis oprit'
$S = Now-Iso
Show 'demo-redis-001-0' GET "/employees/$ID" | Out-Null
$out = Header 'Redis oprit: proxy-ul serveste fara cache; invalidarea pierduta e scrisa la revenire' 'correlation_id=demo-redis-001-*'
$out += '## Raspunsuri (client)'
$out += Show 'demo-redis-001-1' GET "/employees/$ID"
$out += ((Now-Iso) + ' docker compose stop redis'); Docker-Quiet compose stop redis
$out += Show 'demo-redis-001-2' GET "/employees/$ID"
$out += Show 'demo-redis-001-3' PUT "/employees/$ID" (Emp 34000)
$out += Show 'demo-redis-001-4' GET "/employees/$ID"
$out += ((Now-Iso) + ' docker compose start redis'); Docker-Quiet compose start redis
Start-Sleep -Seconds 4
$out += Show 'demo-redis-001-5' GET "/employees/$ID"
$out += Show 'demo-redis-001-6' GET "/employees/$ID"
$out += @('', '## Loguri')
$out += Logs $S 'demo-redis-001|redis_up'
Save "$EV/demo-redis-down-001.log" $out '## Raspunsuri' '## Loguri'

# ---------------------------------------------------------------------------
'8. smart proxy: conexiuni pastrate, registrul din Redis'
$out = Header 'Smart proxy: 5 cereri pe o singura conexiune keep-alive; registrul clientilor si al cererilor in Redis' 'X-Client-ID: demo-client'
$out += '## curl: 5 URL-uri, o singura conexiune TCP (num_connects=1 doar la prima)'
$urls = @("$Proxy/employees/emp-j-001", "$Proxy/employees/emp-j-002", "$Proxy/employees/emp-j-003", "$Proxy/employees/emp-x-001", "$Proxy/employees?limit=3")
$ca = @('-s', '-H', 'X-Client-ID: demo-client', '-H', 'X-Correlation-ID: demo-smart-001', '-w', '%{url_effective} status=%{http_code} num_connects=%{num_connects}\n')
foreach ($u in $urls) { $ca += @('-o', [System.IO.Path]::GetTempFileName(), $u) }
$out += & $script:Curl @ca
Start-Sleep -Seconds 1
$out += @('', '## GET :8081/proxy/requests/demo-smart-001 (ultima cerere cu acest id)')
$out += (Http GET ($Admin + '/proxy/requests/demo-smart-001')).Body
$out += @('', '## GET :8081/proxy/clients')
$out += (Http GET ($Admin + '/proxy/clients')).Body
$out += @('', '## GET :8081/proxy/stats (conns_new / conns_reused: pool-ul spre replici)')
$out += (Http GET ($Admin + '/proxy/stats')).Body
Write-Utf8 "$EV/demo-smart-proxy-001.log" $out
$out | Select-Object -First 12

Reset-Faults
''
"Dovezile sunt in $EV/demo-*.log"
