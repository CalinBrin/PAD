# Helpers shared by smoke.ps1, demo.ps1 and bench.ps1.
# Works with Windows PowerShell 5.1 and PowerShell 7 (Windows, macOS, Linux).
# ASCII only on purpose: Windows PowerShell 5.1 reads scripts without a BOM
# in the ANSI code page.

# 'Continue': in Windows PowerShell 5.1, 'Stop' turns the stderr of native
# commands (docker compose writes its progress there) into terminating errors.
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path -Parent $PSScriptRoot)

# Windows PowerShell 5.1 has "curl" as an alias of Invoke-WebRequest:
# use the real curl (curl.exe ships with Windows 10 1803+).
$script:Curl = 'curl'
if ($PSVersionTable.PSEdition -eq 'Desktop' -or $IsWindows) { $script:Curl = 'curl.exe' }

$script:Utf8 = New-Object System.Text.UTF8Encoding($false)
$script:TmpBody = [System.IO.Path]::GetTempFileName()

function Now-Ms { (Get-Date).ToUniversalTime().ToString('HH:mm:ss.fff') }
function Now-Iso { (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ') }

# Http: one request with curl; returns Status, Headers (lower-case keys), Body.
# The body goes through a temporary file: Windows PowerShell 5.1 strips the
# double quotes of JSON passed as a native argument.
function Http {
    param([string]$Method, [string]$Url, [string]$Body = '', [hashtable]$Headers = @{})
    $a = @('-s', '-i', '-X', $Method)
    foreach ($k in $Headers.Keys) { $a += @('-H', ($k + ': ' + $Headers[$k])) }
    if ($Body -ne '') {
        [System.IO.File]::WriteAllText($script:TmpBody, $Body, $script:Utf8)
        $a += @('-H', 'Content-Type: application/json', '--data-binary', ('@' + $script:TmpBody))
    }
    $a += $Url
    return (Parse-Curl (& $script:Curl @a))
}

# Parse-Curl: the output of "curl -i" (lines) -> Status, Headers, Body.
function Parse-Curl($lines) {
    $r = [pscustomobject]@{ Status = 0; Headers = @{}; Body = '' }
    if (-not $lines) { return $r }
    $lines = @($lines)
    $i = 0
    # skip "HTTP/1.1 100 Continue" blocks
    while ($i -lt $lines.Count) {
        $statusLine = ([string]$lines[$i]).TrimEnd("`r")
        $i++
        if ($statusLine -match '^HTTP/\S+\s+(\d+)') { $r.Status = [int]$Matches[1] }
        # headers end at the blank line, or at the first line that is not a
        # header (Start-Process -RedirectStandardOutput drops blank lines on
        # Linux/macOS)
        while ($i -lt $lines.Count -and ([string]$lines[$i]) -match '^[A-Za-z0-9-]+:') {
            $h = ([string]$lines[$i]).TrimEnd("`r")
            $p = $h.IndexOf(':')
            $r.Headers[$h.Substring(0, $p).ToLower()] = $h.Substring($p + 1).Trim()
            $i++
        }
        if ($i -lt $lines.Count -and ([string]$lines[$i]).TrimEnd("`r") -eq '') { $i++ }
        if ($r.Status -ne 100) { break }
    }
    if ($i -lt $lines.Count) { $r.Body = ($lines[$i..($lines.Count - 1)] -join "`n") }
    return $r
}

function Hdr($r, $name) { if ($r.Headers.ContainsKey($name)) { return $r.Headers[$name] } return '' }

# Format-Answer: one line with the headers that matter and the salary.
function Format-Answer([string]$Method, [string]$Path, $r) {
    $hs = @()
    foreach ($n in @('x-cache', 'x-upstream', 'x-attempts', 'x-outcome', 'x-changed', 'age', 'idempotent-replayed')) {
        if ($r.Headers.ContainsKey($n)) { $hs += ($n + ': ' + $r.Headers[$n]) }
    }
    $m = [regex]::Match($r.Body, '"(salary|code)":[^,}]*')
    '{0}  {1,-6} {2,-28} -> HTTP {3} | {4} | {5}' -f (Now-Ms), $Method, $Path, $r.Status, ($hs -join ' '), $m.Value
}

# Show: send a request through the proxy and format the answer.
function Show {
    param([string]$Cid, [string]$Method, [string]$Path, [string]$Body = '', [hashtable]$Extra = @{})
    $h = @{ 'X-Correlation-ID' = $Cid; 'From' = 'demo-client' }
    foreach ($k in $Extra.Keys) { $h[$k] = $Extra[$k] }
    Format-Answer $Method $Path (Http $Method ($script:Proxy + $Path) $Body $h)
}

function Fault([int]$Replica, [string]$Query) {
    & $script:Curl -fsS -X POST ($script:DwAdmin[$Replica - 1] + '/admin/faults?' + $Query) | Out-Null
}
function Reset-Faults { 1..3 | ForEach-Object { Fault $_ 'reset=1' } }

# Logs: the lines of every component since $Since that match $Pattern, by time.
function Logs([string]$Since, [string]$Pattern) {
    $lines = docker compose logs --no-color --since $Since proxy dw-1 dw-2 dw-3 source-json source-xml 2>$null
    $sel = @($lines | Where-Object { $_ -match $Pattern } | ForEach-Object { $_ -replace '^([a-z0-9-]+)-1\s+\|\s', '$1 | ' })
    $sel | Sort-Object { if ($_ -match 'time=(\S+)') { $Matches[1] } else { '' } }
}

function Write-Utf8([string]$Path, $Lines) {
    [System.IO.File]::WriteAllText((Join-Path (Get-Location) $Path), (($Lines | Out-String) -replace "`r`n", "`n"), $script:Utf8)
}

function Docker-Quiet { & docker @args 2>&1 | Out-Null }
