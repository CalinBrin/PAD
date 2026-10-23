# Demo pentru Windows (PowerShell + Docker Desktop). Rulează din folderul Laborator2:
#   powershell -ExecutionPolicy Bypass -File scripts\demo.ps1
# Sau copiază pașii unul câte unul în PowerShell.
# Fiecare pas folosește un correlation ID propriu, ca logurile lui să poată fi filtrate.

$ErrorActionPreference = "Stop"

function Post-Order($correlationId, $key, $body, [switch]$PassThru) {
    # POST /orders cu Idempotency-Key (identificatorul stabil al cererii) și X-Correlation-ID.
    $r = Invoke-WebRequest -UseBasicParsing -Method Post -Uri "http://localhost:8080/orders" `
        -Headers @{ "Idempotency-Key" = $key; "X-Correlation-ID" = $correlationId } `
        -ContentType "application/json" -Body $body
    Write-Host "HTTP $($r.StatusCode) $($r.Content)"
    if ($PassThru) { return ($r.Content | ConvertFrom-Json) }
}

function Wait-Url($url) {
    # Așteaptă până când un endpoint de sănătate răspunde.
    for ($i = 0; $i -lt 90; $i++) {
        try { Invoke-RestMethod $url -TimeoutSec 2 | Out-Null; return } catch { Start-Sleep -Seconds 1 }
    }
    throw "timeout: $url"
}

function Show-Logs($pattern) {
    # Logul corelat: liniile tuturor serviciilor care conțin correlation ID-ul.
    docker compose logs --no-log-prefix | Select-String $pattern | ForEach-Object { $_.Line } | Sort-Object
}

# =====================================================================
# PASUL 0. Pornire curată
# =====================================================================
# .env conține configurația și secretul PAYMENT_API_TOKEN; nu se comite.
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
# Variabilele de demo pornesc dezactivate.
$env:CRASH_ONCE = ""
$env:BROKER_LOSE_PUBLISH_RESPONSES = "0"
# Ștergem containerele și volumele vechi (date, efecte, marker de crash).
docker compose down -v
# Construim imaginile (codul gRPC se generează din .proto în Dockerfile) și pornim:
# broker, payment, order-service, inventory și 2 replici billing.
docker compose up --build -d
Wait-Url "http://localhost:8080/healthz"
Wait-Url "http://localhost:8082/readyz"
Start-Sleep -Seconds 3
docker compose ps

# =====================================================================
# PASUL 1. Flux nominal
# =====================================================================
# API-ul validează cererea, salvează comanda, publică OrderCreated prin gRPC.
# 202 cu status "published" = brokerul a salvat evenimentul (nu că a fost procesat).
Post-Order "demo-normal-001" "req-normal-001" '{"customer_id":"c1","amount_cents":5000,"currency":"EUR"}'
Start-Sleep -Seconds 2
# Cozile goale: inventory și billing au procesat și au trimis ACK.
Invoke-RestMethod http://localhost:8081/stats | ConvertTo-Json
# Plata executată la serviciul extern, cu același correlation ID.
Invoke-RestMethod http://localhost:8082/charges | ConvertTo-Json

# =====================================================================
# PASUL 2. Aceeași cerere de două ori (idempotency)
# =====================================================================
# Al doilea POST cu aceeași cheie: 200, aceeași comandă, niciun eveniment nou.
Post-Order "demo-idem-001" "req-idem-001" '{"customer_id":"c2","amount_cents":1500}'
Post-Order "demo-idem-001" "req-idem-001" '{"customer_id":"c2","amount_cents":1500}'

# =====================================================================
# PASUL 3. Eroare permanentă: DLQ fără retry
# =====================================================================
# GBP are format valid, dar billing nu o suportă: nack permanent, DLQ după 1 livrare.
Post-Order "demo-dlq-001" "req-dlq-001" '{"customer_id":"c3","amount_cents":700,"currency":"GBP"}'
Start-Sleep -Seconds 2
Invoke-RestMethod "http://localhost:8081/dlq?queue=orders.billing" | ConvertTo-Json -Depth 5

# =====================================================================
# PASUL 4. SCENARIUL CRITIC: răspunsul plății se pierde după efectuarea ei
# =====================================================================
# Injectăm eșecul: următoarea plată se execută și se salvează, dar răspunsul
# este reținut până expiră deadline-ul lui billing (800 ms).
Invoke-RestMethod -Method Post "http://localhost:8082/faults?lose_responses=1"
Post-Order "demo-lost-charge-001" "req-lost-charge-001" '{"customer_id":"c4","amount_cents":4200}'
Start-Sleep -Seconds 4
# Caută în ordine: "charge executed", "FAULT INJECTED", "charge outcome unknown",
# "reconciled: the charge was executed", "order charged ... replayed=true", "message acked".
Show-Logs "demo-lost-charge-001"
# O singură plată pentru comanda din pasul 4, deși răspunsul s-a pierdut.
(Invoke-RestMethod http://localhost:8082/charges) | Where-Object { $_.correlation_id -eq "demo-lost-charge-001" } | ConvertTo-Json

# =====================================================================
# PASUL 5. Răspunsul la Publish se pierde după commit
# =====================================================================
# Repornim brokerul cu injectarea activă pentru o publicare (datele rămân în volum).
$env:BROKER_LOSE_PUBLISH_RESPONSES = "1"
docker compose up -d broker
Wait-Url "http://localhost:8081/readyz"
Start-Sleep -Seconds 3
# order-service primește DeadlineExceeded, reîncearcă cu același message_id,
# brokerul răspunde duplicate=true: un singur eveniment.
Post-Order "demo-lost-publish-001" "req-lost-publish-001" '{"customer_id":"c5","amount_cents":900}'
Start-Sleep -Seconds 2
Show-Logs "demo-lost-publish-001"
# Dezactivăm injectarea.
$env:BROKER_LOSE_PUBLISH_RESPONSES = "0"
docker compose up -d broker
Wait-Url "http://localhost:8081/readyz"
Start-Sleep -Seconds 3

# =====================================================================
# PASUL 6. Consumatorul cade după efect, înainte de ACK
# =====================================================================
# inventory va cădea o singură dată (exit 137) la următorul mesaj, după ce scrie efectul.
$env:CRASH_ONCE = "after_effect"
docker compose up -d inventory
Start-Sleep -Seconds 4
Post-Order "demo-crash-001" "req-crash-001" '{"customer_id":"c6","amount_cents":120}'
# Docker repornește inventory; fișierul marker îl împiedică să cadă din nou;
# brokerul îi relivrează mesajul (attempt=2) și el îl recunoaște ca duplicat.
Start-Sleep -Seconds 8
Show-Logs "demo-crash-001"
# O singură linie de efect pentru comanda din pasul 6.
docker compose exec inventory cat /data/effects.jsonl | Select-String "demo-crash-001"

# =====================================================================
# PASUL 7. Brokerul indisponibil: fallback prin outbox
# =====================================================================
docker compose stop broker
# Comanda este salvată ca "pending"; clientul primește 202 pending, nu o eroare.
$o = Post-Order "demo-outbox-001" "req-outbox-001" '{"customer_id":"c7","amount_cents":990}' -PassThru
# API-ul raportează dependența indisponibilă.
Invoke-RestMethod http://localhost:8080/readyz | ConvertTo-Json
docker compose start broker
Wait-Url "http://localhost:8081/readyz"
Start-Sleep -Seconds 6
# Outbox-ul a republicat comanda cu același message_id: status "published".
Invoke-RestMethod "http://localhost:8080/orders/$($o.order_id)" | ConvertTo-Json

# =====================================================================
# PASUL 8. Persistență: mesajul supraviețuiește repornirii brokerului
# =====================================================================
docker compose stop inventory
Post-Order "demo-persist-001" "req-persist-001" '{"customer_id":"c8","amount_cents":10}'
Start-Sleep -Seconds 1
# orders.inventory: pending 1, consumers 0.
Invoke-RestMethod http://localhost:8081/stats | ConvertTo-Json
docker compose restart broker
Wait-Url "http://localhost:8081/readyz"
docker compose start inventory
Start-Sleep -Seconds 5
# "local effect applied" pentru demo-persist-001: mesajul a fost refăcut din log.
docker compose logs inventory | Select-String "demo-persist-001"

# =====================================================================
# PASUL 9. Load balancing între cele două replici billing
# =====================================================================
1..10 | ForEach-Object { Post-Order ("demo-lb-{0:d2}" -f $_) ("req-lb-{0:d2}" -f $_) '{"customer_id":"c9","amount_cents":100}' }
Start-Sleep -Seconds 4
foreach ($r in 1, 2) {
    $n = (docker compose logs billing | Select-String "billing-$r\s.*demo-lb-.*event=charge_approved").Count
    Write-Host "billing-$r a încasat $n din cele 10 comenzi"
}

# =====================================================================
# PASUL 10. Testele automate
# =====================================================================
docker compose run --rm tests

# =====================================================================
# PASUL 11. Oprire
# =====================================================================
# docker compose down -v
