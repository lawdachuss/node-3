param(
    [Parameter(Mandatory = $false)]
    [string]$Org = "lawdachuss",

    [Parameter(Mandatory = $false)]
    [string]$Prefix = "node",

    [Parameter(Mandatory = $false)]
    [int]$Count = 18,

    [Parameter(Mandatory = $false)]
    [int]$Limit = 50,

    [Parameter(Mandatory = $false)]
    [int]$ThrottleLimit = 10,

    [Parameter(Mandatory = $false)]
    [int]$WaitTimeout = 180
)

function Invoke-Parallel($items, $scriptBlock, $throttle, $extraArg = $null) {
    $results = @()
    $jobs = @()
    $queue = [System.Collections.Queue]::new()
    foreach ($item in @($items)) { $queue.Enqueue($item) }

    while ($queue.Count -gt 0 -or $jobs.Count -gt 0) {
        while ($jobs.Count -lt $throttle -and $queue.Count -gt 0) {
            $item = $queue.Dequeue()
            if ($extraArg) {
                $job = Start-Job -ScriptBlock $scriptBlock -ArgumentList $item, $extraArg
            } else {
                $job = Start-Job -ScriptBlock $scriptBlock -ArgumentList $item
            }
            $jobs += @{ Job = $job; Item = $item }
        }

        $done = @($jobs | Where-Object { $_.Job.State -ne 'Running' })
        foreach ($j in $done) {
            $result = $j.Job | Receive-Job
            $results += @($result)
            $j.Job | Remove-Job
            $jobs = @($jobs | Where-Object { $_.Job -ne $j.Job })
        }

        if ($jobs.Count -ge $throttle -or $queue.Count -eq 0) {
            Start-Sleep -Milliseconds 200
        }
    }
    return $results
}

$repoList = 1..$Count | ForEach-Object { "${Org}/${Prefix}-${_}" }

Write-Host "=== Scanning and cancelling active workflows (last $Limit runs per repo, parallel=$ThrottleLimit) ==="
Write-Host "Org: $Org, Prefix: $Prefix, Count: $Count"

$cancelScript = {
    param($repo, $limit)
    $repoName = $repo.Split('/')[-1]
    $result = gh run list --repo $repo --limit $limit --json databaseId,status,conclusion 2>&1
    $cancelledIds = @()
    if ($LASTEXITCODE -eq 0 -and $result) {
        $parsedRuns = $result | ConvertFrom-Json
        $runs = @($parsedRuns)
        foreach ($run in $runs) {
            if ($run.status -ne "completed") {
                gh run cancel $run.databaseId --repo $repo 2>&1 | Out-Null
                if ($LASTEXITCODE -eq 0) {
                    $cancelledIds += @($run.databaseId)
                }
            }
        }
    }
    if ($cancelledIds.Count -gt 0) {
        Write-Host ($repoName + ": cancelling " + $cancelledIds.Count + " run(s) - IDs: " + ($cancelledIds -join ', '))
    } else {
        Write-Host ($repoName + ": no active runs in last " + $limit)
    }
    return [pscustomobject]@{ Repo = $repo; RepoName = $repoName; Cancelled = $cancelledIds }
}

$cancelResults = Invoke-Parallel $repoList $cancelScript $ThrottleLimit $Limit

$allCancelledIds = @{}
foreach ($r in @($cancelResults)) {
    if ($r.Cancelled.Count -gt 0) {
        $allCancelledIds[$r.Repo] = $r.Cancelled
    }
}

Write-Host ""
Write-Host "=== Waiting for cancelled runs to fully stop (parallel) ==="
$waitItems = @()
foreach ($repo in $repoList) {
    if ($allCancelledIds.ContainsKey($repo)) {
        $repoName = $repo.Split('/')[-1]
        foreach ($id in @($allCancelledIds[$repo])) {
            $waitItems += [pscustomobject]@{ RunId = $id; Repo = $repo; RepoName = $repoName }
        }
    }
}

if ($waitItems.Count -gt 0) {
    $waitScript = {
        param($item, $timeoutSeconds)
        $runId = $item.RunId
        $repo = $item.Repo
        $deadline = (Get-Date).AddSeconds($timeoutSeconds)
        while ((Get-Date) -lt $deadline) {
            $info = gh run view $runId --repo $repo --json status,conclusion 2>&1 | Out-Null
            if ($LASTEXITCODE -eq 0) {
                $parsed = gh run view $runId --repo $repo --json status,conclusion | ConvertFrom-Json
                if ($parsed.status -eq "completed") {
                    return [pscustomobject]@{ RunId = $runId; Repo = $repo; RepoName = $item.RepoName; Finished = $true }
                }
            }
            Start-Sleep -Seconds 3
        }
        return [pscustomobject]@{ RunId = $runId; Repo = $repo; RepoName = $item.RepoName; Finished = $false }
    }

    $waitResults = Invoke-Parallel $waitItems $waitScript $ThrottleLimit $WaitTimeout

    foreach ($r in @($waitResults)) {
        if ($r.Finished) {
            Write-Host ($r.RepoName + ": run " + $r.RunId + " stopped")
        } else {
            Write-Host ($r.RepoName + ": run " + $r.RunId + " still active after timeout, proceeding anyway")
        }
    }
}

Write-Host ""
Write-Host "=== Triggering fresh workflows in all repos (parallel) ==="
$triggerScript = {
    param($repo)
    $repoName = $repo.Split('/')[-1]
    $result = gh workflow run secure-rdp.yml --repo $repo 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host ($repoName + ": triggered successfully")
        return [pscustomobject]@{ Repo = $repo; Success = $true }
    } else {
        Write-Host ($repoName + ": trigger failed - " + $result)
        return [pscustomobject]@{ Repo = $repo; Success = $false; Error = $result }
    }
}

$triggerResults = Invoke-Parallel $repoList $triggerScript $ThrottleLimit

$failed = @($triggerResults | Where-Object { -not $_.Success })
if ($failed.Count -gt 0) {
    Write-Host ""
    Write-Host "=== Failed triggers: $($failed.Count) ==="
    $failed | ForEach-Object { Write-Host ($_.Repo.Split('/')[-1] + ": " + $_.Error) }
}
