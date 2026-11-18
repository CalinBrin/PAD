# Quick check that the running system works (about 20 seconds).
# Usage (Windows):  powershell -ExecutionPolicy Bypass -File scripts\smoke.ps1
#       (pwsh):     pwsh scripts/smoke.ps1
# The system must be started first: docker compose up --build -d
param(
    [string]$Proxy = 'http://localhost:8080',
    [string]$Admin = 'http://127.0.0.1:8081'
)
. (Join-Path $PSScriptRoot 'lib.ps1')
$script:Proxy = $Proxy

$ok = 0; $total = 0
function Check([string]$Name, [bool]$Cond, [string]$Detail = '') {
    $script:total++
    if ($Cond) { $script:ok++; Write-Host ('  [OK]   ' + $Name + '  ' + $Detail) -ForegroundColor Green }
    else { Write-Host ('  [FAIL] ' + $Name + '  ' + $Detail) -ForegroundColor Red }
}

Write-Host '1. Proxy and warehouse replicas'
$healthy = 0
for ($i = 0; $i -lt 30; $i++) {
    $s = Http GET ($Admin + '/proxy/stats')
    $healthy = ([regex]::Matches($s.Body, '"healthy": true')).Count
    if ($healthy -ge 3) { break }
    Start-Sleep -Seconds 2
}
Check 'proxy answers, 3 replicas in rotation' ($healthy -ge 3) ("healthy=$healthy")

Write-Host '2. GET through the proxy: MISS, then HIT (cache in Redis)'
$id = 'emp-j-001'
$r1 = Http GET "$Proxy/employees/$id" '' @{ 'Cache-Control' = 'no-cache' }
$r2 = Http GET "$Proxy/employees/$id"
Check 'first GET reaches a replica' ($r1.Status -eq 200 -and (Hdr $r1 'x-cache') -eq 'MISS') ('X-Cache=' + (Hdr $r1 'x-cache') + ' X-Upstream=' + (Hdr $r1 'x-upstream'))
Check 'second GET served from cache' ($r2.Status -eq 200 -and (Hdr $r2 'x-cache') -eq 'HIT') ('X-Cache=' + (Hdr $r2 'x-cache') + ' Age=' + (Hdr $r2 'age'))

Write-Host '3. JSON or XML, chosen by the client'
$x = Http GET "$Proxy/employees/emp-x-002" '' @{ 'Accept' = 'application/xml' }
$j = Http GET "$Proxy/employees/emp-x-002" '' @{ 'Accept' = 'application/json' }
Check 'Accept: application/xml gives XML' ($x.Body.StartsWith('<?xml')) (Hdr $x 'content-type')
Check 'Accept: application/json gives JSON' ($j.Body.StartsWith('{')) (Hdr $j 'content-type')

Write-Host '4. Routes of the statement'
$l = Http GET "$Proxy/employees/?offset=1&limit=4"
$e = Http GET "$Proxy/employee/?id=emp-j-003"
Check 'GET /employees/?offset=1&limit=4' ($l.Status -eq 200 -and $l.Body -match '"offset":1,"limit":4') ([regex]::Match($l.Body, '"total":\d+').Value)
Check 'GET /employee/?id=emp-j-003' ($e.Status -eq 200 -and $e.Body -match '"id":"emp-j-003"')

Write-Host '5. Round robin over the 3 replicas'
$seen = @{}
1..6 | ForEach-Object {
    $r = Http GET "$Proxy/employees/emp-j-002" '' @{ 'Cache-Control' = 'no-cache' }
    $seen[(Hdr $r 'x-upstream')] = 1
}
Check 'requests spread over dw-1, dw-2, dw-3' ($seen.Count -eq 3) (($seen.Keys | Sort-Object) -join ' ')

Write-Host '6. Client modification (POST, Idempotency-Key) pulled by the XML source node'
$salary = Get-Random -Minimum 20000 -Maximum 90000
$key = 'smoke-' + [guid]::NewGuid().ToString('N').Substring(0, 8)
$since = Now-Iso
$p1 = Http POST "$Proxy/employees/emp-x-003" ('{"salary":' + $salary + '}') @{ 'Idempotency-Key' = $key; 'From' = 'smoke-test' }
$p2 = Http POST "$Proxy/employees/emp-x-003" ('{"salary":' + $salary + '}') @{ 'Idempotency-Key' = $key; 'From' = 'smoke-test' }
Check 'POST applied' ($p1.Status -eq 200 -and (Hdr $p1 'x-changed') -eq 'true') ("salary=$salary")
Check 'same POST again: replayed, no new effect' ((Hdr $p2 'idempotent-replayed') -eq 'true' -and (Hdr $p2 'x-changed') -eq 'false')
$g = Http GET "$Proxy/employees/emp-x-003"
Check 'GET after POST sees the new value (cache invalidated)' ($g.Body -match ('"salary":' + $salary)) ('X-Cache=' + (Hdr $g 'x-cache'))
$pulled = $false
for ($i = 0; $i -lt 8 -and -not $pulled; $i++) {
    Start-Sleep -Seconds 1
    $pulled = [bool](docker compose logs --no-color --since $since source-xml 2>$null | Select-String ('change_applied id=emp-x-003 .*salary=' + $salary))
}
Check 'source-xml pulled the modification' $pulled

Write-Host '7. Smart proxy'
$s = Http GET ($Admin + '/proxy/stats')
$reused = [regex]::Match($s.Body, '"conns_reused": (\d+)').Groups[1].Value
$new = [regex]::Match($s.Body, '"conns_new": (\d+)').Groups[1].Value
Check 'connections to the replicas are reused' ([int64]$reused -gt [int64]$new) ("new=$new reused=$reused")
$c = Http GET ($Admin + '/proxy/clients')
Check 'client registry kept in Redis' ($c.Body -match '"source": "redis"') ([regex]::Match($c.Body, '"requests": \d+').Value)

Write-Host ''
if ($ok -eq $total) { Write-Host "All $total checks passed." -ForegroundColor Green; exit 0 }
Write-Host "$ok of $total checks passed." -ForegroundColor Red
exit 1
