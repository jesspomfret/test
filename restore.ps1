param (
  [string]$primary,
  [string]$secondary,
  [string]$agname,
  [string[]]$databases,
  [string]$backupPath,
  [DateTime]$restoreToTime,
  [string]$databaseOwner,
  [int]$threads = 5,
  [switch]$slackNotification,
  [string]$slackUri
  )

  if($slackNotification -and -not $slackUri) {
    throw 'You must provide a slackUri when using slackNotification'
  }
  
  ## write to slack or somewhere with results1
  if ($slackNotification) {
    $payload = @{
      "text"  = ('Starting perf refresh for {0} to {1}' -f ($databases -join ', '), $restoreToTime)
    }

    $webSplat = @{
      UseBasicParsing = $true
      Body            = (ConvertTo-Json -Compress -InputObject $payload)
      Method          = 'Post'
      Uri             = $SlackUri
    }
    Invoke-WebRequest @webSplat
  }


# set up logging
$paramSetPSFLoggingProvider = @{
  Name                = 'logfile'
  InstanceName        = 'perfag01'
  FilePath            = 'C:\temp\sqladmin\Jira Tickets\SQLDEV-379\log\perfRefresh-%Date%.csv'
  LogRotatePath       = 'C:\temp\sqladmin\Jira Tickets\SQLDEV-379\log\perfRefresh-*.csv'
  LogRetentionTime    = '60d'
  Enabled             = $true
}
Set-PSFLoggingProvider @paramSetPSFLoggingProvider

Write-PSFMessage ('starting process') -Level Output
  
# dot source the parallel script
. .\Invoke-Parallel.ps1

$params = [pscustomobject]@{
  primary       = $primary
  secondary     = $secondary
  agname        = $agname
  databases     = $databases
  backupPath    = $backupPath
  restoreToTime = $restoreToTime
  databaseOwner = $databaseOwner
  credential    = $credential
}

$databases | Invoke-Parallel -Throttle $threads -parameter $params {
  $db = $_

  Import-Module dbatools, PSFramework
                        
  # unwrap params
  $primary = $parameter.primary
  $secondary = $parameter.secondary
  $agname = $parameter.agname
  $backupPath = $parameter.backupPath
  $restoreToTime = $parameter.restoreToTime
  $databaseOwner = $parameter.databaseOwner

  # Set the configurations to old defaults for SMO connections
  Set-DbatoolsConfig -FullName sql.connection.trustcert -Value $true -PassThru | Register-DbatoolsConfig
  Set-DbatoolsConfig -FullName sql.connection.encrypt -Value $false -PassThru | Register-DbatoolsConfig
  
  Write-PsfMessage -Message ('[{0}] - Starting restore for {0}' -f $db) -level Output
  
  ## remove database from AG
  try {
    Write-PsfMessage -Message ('[{0}] - Removing {0} from availability group {1}' -f $db, $agname) -level output
    $null = Remove-DbaAgDatabase -SqlInstance $primary -database $db -AvailabilityGroup $agname -Confirm:$false -EnableException
  } catch {
    Stop-PSFFunction -Message ('[{0}] - Issue removing {0} - from availability group {1}' -f $db, $agname) -ErrorRecord $_ -Continue
  }
  
  ##restore database on primary
  try {
    Write-PsfMessage -Message ('[{0}] - Restore database {0} to primary server {1} to {2}' -f $db, $primary, $restoreToTime) -level output

    # check current restoreToPoint
    $maxRestorePointQuery = ("select max(stop_at) as MaxRestoreTime from MSDB.DBO.restorehistory rh inner join msdb.dbo.backupset bs on rh.backup_set_id = bs.backup_set_id where destination_database_name = '{0}'" -f $db)
    [datetime]$maxRestorePoint = (Invoke-DbaQuery -SqlInstance pfvm04vl133 -Query $maxRestorePointQuery).MaxRestoreTime

    if ($maxRestorePoint -eq $restoreToTime) {
          Write-PsfMessage -Message ('[{0}] - Database already restored to {0} on primary server {1}' -f $restoreToTime, $primary) -level output
    } else {
      $restoreParams = @{
        SqlInstance = $primary
        DatabaseName = $db
        Path = (Join-Path -Path $backupPath -ChildPath $db)
        RestoreTime = $restoreToTime
        WithReplace = $true
        ReuseSourceFolderStructure = $true
        EnableException = $true
        NoRecovery = $true # don't bring either side online
      }
      $restored = Restore-DbaDatabase @restoreParams
      
      if(-not $restored) {
        Stop-PSFFunction -Message ('[{0}] - Nothing found to restore for {0} to {1}' -f $db, $primary) -EnableException:$true
      }
      # bring it online on the primary
      $null = Restore-DbaDatabase -SqlInstance $primary -DatabaseName $db -Recover -EnableException
    }
  } catch {
    Stop-PSFFunction -Message ('[{0}] - Issue restoring {0} - to primary server' -f $db, $primary) -ErrorRecord $_ -Continue
  }

    ##restore database on secondary
    try {
      Write-PsfMessage -Message ('[{0}] - Restore database {0} to secondary server {1} to {2}' -f $db, $secondary, $restoreToTime) -level output
      
      # TODO: change to use join-path when we're on windows (currently running on windows against linux we get slashes all over the place)
      $restoreParams = @{
        SqlInstance = $secondary
        DatabaseName = $db
        Path = (Join-Path -Path $backupPath -ChildPath $db)
        RestoreTime = $restoreToTime
        WithReplace = $true
        ReuseSourceFolderStructure = $true
        EnableException = $true
        NoRecovery = $true # don't bring either side online
      }
      $restored = Restore-DbaDatabase @restoreParams
  
      if(-not $restored) {
        Stop-PSFFunction -Message ('[{0}] - Nothing found to restore for {0} on {1}' -f $db, $secondary) -EnableException:$true
      }
  
    } catch {
      Stop-PSFFunction -Message ('[{0}] - Issue restoring {0} - to secondary server' -f $db, $secondary) -ErrorRecord $_ -Continue
    }
  
  ## change database owner to sa
  try {
    Write-PsfMessage -Message ('[{0}] - Set database owner for {0} to {1} on {2}' -f $db, $databaseOwner, $primary) -level output
    $null = Set-DbaDbOwner -SqlInstance $primary -Database $db -TargetLogin $databaseOwner -EnableException
  } catch {
    Stop-PSFFunction -Message ('[{0}] - Issue setting database owner for {0} to {1} on primary server {2}' -f $db, $databaseOwner, $primary) -ErrorRecord $_ -Continue
  }  

  ## add database back into AG with auto seeding
  try {
    Write-PsfMessage -Message ('[{0}] - Add database {0} to {1} on {2}' -f $db, $agname, $primary) -level output
    $null = Add-DbaAgDatabase -SqlInstance $primary -Database $db -AvailabilityGroup $agname -EnableException
  } catch {
    Stop-PSFFunction -Message ('[{0}] - Issue adding database {0} to ag {1} on primary server {2}' -f $db, $agname, $primary) -ErrorRecord $_ -Continue
  }

  Write-PsfMessage -Message ('[{0}] - Completed process for database {0}' -f $db) -level output
}
  ## execute 2 sql jobs
  #TODO: add jobs

  ## collect any errors for notifications
$errorMessages = ''
if (Get-PSFMessage -Errors) {
  Get-PSFMessage -Errors | ForEach-Object {
    $errorMessages += (" `r`n - {0} - {1}" -f $_.Timestamp, $_.Message)
  }
}

## run tests
# exit with exit code equal to number of failed tests and pass in data for tests
$container = New-PesterContainer -Path '.\tests\' -Data @{ restoreToTime = $restoreToTime; primary = $primary; secondary = $secondary; agname = $agname; databases = $databases; databaseOwner = $databaseOwner}
$config = New-PesterConfiguration
#$config.Run.Exit = $true
$config.Run.Container = $container
$config.TestResult.Enabled = $true
$config.TestResult.OutputPath = '.\log\testresults.xml'
$config.Output.Verbosity = 'Detailed'
$config.Run.PassThru = $true

$tests = Invoke-Pester -Configuration $config

if($tests.Result -eq 'Passed') {
  Write-PsfMessage -Message ('All {0} tests passed' -f $tests.TotalCount) -level output
  $message = ('Perf refresh complete. All {0} tests passed' -f $tests.TotalCount)

  if ($errorMessages) {
    $message += " `r`n :x: But there were errors:"
    $message += $errorMessages
  }

} else {
  Write-PsfMessage -Message ('{0} tests failed' -f $tests.FailedCount) -level output
  $message = (":fire: - Perf refresh completed but {0} tests have failed!" -f $tests.FailedCount)

  # output failed tests
  $tests.Failed | ForEach-Object {
    Write-PsfMessage -Message ('{0} - {1}' -f $_.ExpandedName, $_.ErrorRecord[0]) -level Error
       
    $message += (" `r`n - {0} - {1}" -f $_.ExpandedName, $_.ErrorRecord[0])
  }

  if ($errorMessages) {
    $message += " `r`n :x: These errors were caught in the process:"
    $message += $errorMessages
  }
}

## write to slack or somewhere with results1
if ($slackNotification) {
  $payload = @{
    "text"  = $message
  }

  $webSplat = @{
    UseBasicParsing = $true
    Body            = (ConvertTo-Json -Compress -InputObject $payload)
    Method          = 'Post'
    Uri             = $SlackUri
  }
  Invoke-WebRequest @webSplat
}  

#Yesterday (29/01/2024) - the process ran from 1015 - 1530. Which is 5 Hours 15 mins
#TODO: slack notification
#TODO: disables all ap_* logins before and re-enables after
#TODO: stops SQL Agent service and restarts after
#TODO: pester tests to ensure ag is healthy
#TODO: pester tests to ensure access is in place
#TODO: pester tests to ensure post_restore_changes worked as expected
