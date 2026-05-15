param(
    [string]$TargetDate = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd"),
    [string]$Config = "config/quant_data_center.yaml",
    [switch]$CreateGitHubIssue,
    [string[]]$IssueLabels = @("data-quality", "qdc")
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$LogsDir = Join-Path $RepoRoot "data\quant_data_center\logs"
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

function Invoke-Qdc {
    param([string[]]$QdcArgs)

    & conda run -n ai-trader qdc --config $Config @QdcArgs
    return $LASTEXITCODE
}

function New-QualityIssue {
    param([string]$ReportPath)

    if (-not $CreateGitHubIssue -and $env:QDC_CREATE_GITHUB_ISSUE -ne "1") {
        Write-Host "Quality report written to $ReportPath. Set QDC_CREATE_GITHUB_ISSUE=1 or pass -CreateGitHubIssue to open a GitHub Issue."
        return
    }

    if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
        Write-Host "GitHub CLI is not available; skipped GitHub Issue creation."
        return
    }

    $Title = "[QDC] Data quality failure: $TargetDate"
    $ExistingIssue = & gh issue list --state open --search "$Title in:title" --json number --jq ".[0].number"
    if ($LASTEXITCODE -eq 0 -and $ExistingIssue) {
        Write-Host "Existing GitHub Issue #$ExistingIssue already matches $Title."
        return
    }

    $LabelArgs = @()
    foreach ($Label in $IssueLabels) {
        if ($Label) {
            $LabelArgs += @("--label", $Label)
        }
    }
    & gh issue create --title $Title --body-file $ReportPath @LabelArgs
}

$Failed = $false

$Steps = @(
    @("crawl-daily", "--date", $TargetDate),
    @("build-factors", "--factor-set", "all", "--start", $TargetDate, "--end", $TargetDate),
    @("sync-parquet", "--layer", "all")
)

foreach ($Step in $Steps) {
    $ExitCode = Invoke-Qdc -QdcArgs $Step
    if ($ExitCode -ne 0) {
        $Failed = $true
        Write-Host "QDC step failed: $($Step -join ' ')"
        break
    }
}

if (-not $Failed) {
    $QualityExitCode = Invoke-Qdc -QdcArgs @("quality", "--start", $TargetDate, "--end", $TargetDate)
    if ($QualityExitCode -ne 0) {
        $ReportPath = Join-Path $LogsDir "quality-issue-$TargetDate.md"
        & conda run -n ai-trader qdc --config $Config quality-issue-report --start $TargetDate --end $TargetDate --output $ReportPath
        New-QualityIssue -ReportPath $ReportPath
        exit $QualityExitCode
    }
}

if ($Failed) {
    exit 1
}

exit 0
