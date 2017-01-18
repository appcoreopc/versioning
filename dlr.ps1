#
# A powershell script to read from any ActiveMq provider
# 
# $MyCredential=New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "admin", (Get-Content $File | ConvertTo-SecureString -Key $key)
#
#

Add-Type -Path Apache.NMS.dll

$global:queueName = "asbhomequote"
$global:timer = New-Object System.Timers.Timer(5000)
$global:runCount = 0
$global:msmqHost = "activemq:tcp://localhost:61616"
$global:daysOld = 0
$global:connected = $false

$action = {
    Write-Host "Timer elapsed at : $((Get-Date).ToString())" 
    Write-Host "Stopping timer execution...."
    $timer.Stop();
    GetActiveQueueMessage($msmqHost)
} 

function global:logToFile([String] $content)
{   
    [System.IO.File]::AppendText("", $content)
}

function global:GetActiveQueueMessage($url)
{
    
    $connection = CreateConnection

    $session = $connection.CreateSession()
    $target = [Apache.NMS.Util.SessionUtil]::GetDestination($session, "queue://$queueName")
    Write-Host "created session  $target  . $target.IsQueue " 

    # creating message queue consumer. 
    # using the Listener - event listener might not be suitable 
    # as we only logs expired messages in the queue. 

    $consumer =  $session.CreateConsumer($target)
    $targetQueue = $session.GetQueue($queueName)
    $queueBrowser = $session.CreateBrowser($targetQueue)
    $messages = $queueBrowser.GetEnumerator()
    
    Write-Host "------------Connection starts---------------"

     $connection.Start()
    
    if ($messages.moveNext())
    {
        $currentMessage = $messages.Current
        $messageTimestamp = getLocalDateTime $currentMessage.Timestamp

        $messageDays = $(Get-Date).Subtract($messageTimestamp).Days
        Write-Host "MessageDays: $messageDays" -ForegroundColor Blue

        $imsg = $consumer.Receive([TimeSpan]::FromMilliseconds(2000))
        Write-Host "Reading message"
        Write-Host $imsg -ForegroundColor Green
     }   

     Write-Host "Closing connection"
     $connection.Close()
     RestartTimer
}

function global:CreateConnection()
{
    write-host "Start connecting to activeMq at : $((Get-Date).ToString())" 
    $uri = [System.Uri]$url
    write-host $uri
    $factory =  New-Object Apache.NMS.NMSConnectionFactory($uri)

    # Getting user credentials and key info
    $key = [Byte[]] $key = (1..16) # will be an input from users, harcoding for now. 
    $File = ".\password.txt"

    if (![System.IO.File]::Exists($path) -eq $false)  
    {
         Write-Host "Unable to find credential file." 
         Exit
    }
   
    $MyCredential=New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "admin", (Get-Content $File | ConvertTo-SecureString -Key $key)
    $username = $MyCredential.UserName.toString()
    $password = $MyCredential.GetNetworkCredential().Password
    Write-Host "Logging in as : $username"

    try {
        $connection = $factory.CreateConnection($username, $password)
        Write-Host $connection
        return $connection
    }
    catch {
        Write-Host "Connection to activeMq : $_.Exception.Message."
    }

    return $null
}

function global:RestartTimer()
{
    Write-Host "Restarting timer."
    $timer.Start();
}

function main()
{   
    setupTimer 
}

function setupTimer()
{
    Write-Host "register timer event"
    Register-ObjectEvent $timer -EventName Elapsed -Action $action
    Write-Host "starting timer"
    $timer.Start()    
}

function global:getLocalDateTime($time)
{
    $timeStr = $time.toString()
    $unixTime = $timeStr.subString(0, $timeStr.Length - 3)
    $epoch = New-Object -Type DateTime -ArgumentList 1970, 1, 1, 0, 0, 0, 0
    return $epoch.AddSeconds($unixTime).ToLocalTime()
}

# Execute main powershell module 
main 
