#
# A powershell script to read from any ActiveMq provider
# $MyCredential=New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "admin", (Get-Content $File | ConvertTo-SecureString -Key $key)
#
#
# Parameters
param(
    [Parameter(Mandatory=$true)]
    [string]$outfolder,
    [Parameter(Mandatory=$true)]
    [string]$hostname,
    [Parameter(Mandatory=$true)]
    [string]$myQueue, 
    [Parameter(Mandatory=$true)]
    [string]$encryptionKey, 
    [Parameter(Mandatory=$true)]
    [string]$username 
    )

Add-Type -Path Apache.NMS.dll

$global:queueName = ""
$global:timer = New-Object System.Timers.Timer(5000)
$global:runCount = 0
$global:msmqHost = ""
$global:daysOld = 0  # age of message in day
$global:connected = $false
$global:logOutputFolder = "" 
$global:encryptionKey;
$global:username;

function global:WriteMessage($queueMessage)
{   
   FindCorrelationId($queueMessage)
}

function global:FindCorrelationId($queueMessage)
{
    $correlationMatch = [regex]::matches($queueMessage.Text, 'CorrelationId="\w+-\w+-\w+-\w+-\w+"')
        if ($correlationMatch.Success -eq $true)
        {
        $correlationId = [regex]::matches($correlationMatch.Value, '"([^"]*)"').Groups[1].Value
        Write-Host $$correlationId
        LogToFile $logOutputFolder $correlationId $queueMessage.Text
        }  
        else 
        {
            $xmlCorrelationMatch = [regex]::matches($queueMessage.Text, 'correlationId>\w+-\w+-\w+-\w+-\w+<')
            if ($xmlCorrelationMatch.Success -eq $true)
            {
                $correlationId = [regex]::matches($xmlCorrelationMatch.Value, '>([^>]*)<').Groups[1].Value 
                Write-Host $$correlationId
                LogToFile $logOutputFolder $correlationId $queueMessage.Text
            }
        }
}

function global:LogToFile([String] $path, [String]$fileName, $content)
{  
    try { 
        $fileToWrite = "$path\$fileName.log"
        [System.IO.File]::AppendAllText($fileToWrite, $content)
    }
    catch 
    {
        Write-Host "File write exception : " $_.Exception.Message -ForegroundColor red
    }
}

function global:GetActiveQueueMessage($activeMqHostUrl)
{   
    Write-Host "Connecting to the following activemq : $activeMqHostUrl" -ForegroundColor Cyan
    # Create connection
    $connection = CreateConnection $activeMqHostUrl

    try  { 
            $session = $connection.CreateSession()
            $target = [Apache.NMS.Util.SessionUtil]::GetDestination($session, "queue://$queueName")
            Write-Host "Created session  $target  . $target.IsQueue " 

            # creating message queue consumer. 
            # using the Listener - event listener might not be suitable 
            # as we only logs expired messages in the queue. 

            $consumer =  $session.CreateConsumer($target)
            $targetQueue = $session.GetQueue($queueName)
            $queueBrowser = $session.CreateBrowser($targetQueue)
            $messages = $queueBrowser.GetEnumerator()

            $connection.Start()
            
            if ($messages.moveNext())
            {
                $currentMessage = $messages.Current
                $messageTimestamp = GetLocalDateTime $currentMessage.Timestamp
                Write-Host $currentMessage

                $messageDays = $(Get-Date).Subtract($messageTimestamp).Days
                Write-Host "MessageDays: $messageDays" -ForegroundColor Blue

                $imsg = $consumer.Receive([TimeSpan]::FromMilliseconds(2000))
                Write-Host "Receiving messages"
                #Write-Host $imsg -ForegroundColor Green

                if ($imsg -ne $null) 
                {
                    WriteMessage($imsg)
                }
            }   

            Write-Host "Closing connection."
            $connection.Close()
            RestartTimer
    }
    catch {
        Write-Host "Core module error : $_.Exception.Message."
    }
    finally {
    }
}

function global:CreateConnection($targetConnectionUrl)
{
    Write-Host "Start connecting to $targetConnectionUrl at : $((Get-Date).ToString())" 
    $uri = [System.Uri]$targetConnectionUrl
    Write-Host "Target activeMq location : $uri"
    $factory =  New-Object Apache.NMS.NMSConnectionFactory($uri)

    # Getting user credentials and key info
    $key = [System.Text.Encoding]::UTF8.GetBytes($encryptionKey.Trim())
    $File = ".\Password.txt"

    if (![System.IO.File]::Exists($path) -eq $false)  
    {
         Write-Host "Unable to find credential 'Password.txt' file. Please ensure this file is in the current folder."  -ForegroundColor Red
         Exit
    }
   
    $MyCredential=New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $username, (Get-Content $File | ConvertTo-SecureString -Key $key)
    $username = $MyCredential.UserName.toString()
    $password = $MyCredential.GetNetworkCredential().Password

    #$username = "admin"
    #$password = "admin"

    try {
        $connection = $factory.CreateConnection($username, $password)
        Write-Host "Successfully connect to : $connection" -ForegroundColor Green
        return $connection
    }
    catch {
        Write-Host "Connection to activeMq : $_.Exception.Message." -ForegroundColor Red
    }

    return $null
}

function global:RestartTimer()
{
    Write-Host "Restarting timer."
    $timer.Start();
}


function SetupTimer()
{
    Write-Host "Register timer event."
    
    Register-ObjectEvent $timer -EventName Elapsed -Action  {
        Write-Host "Timer elapsed at : $((Get-Date).ToString())" 
        Write-Host "Stopping timer execution"
        $timer.Stop();
        Write-Host "Get messages from $msmqHost"
        GetActiveQueueMessage $msmqHost
    } 

    Write-Host "Restarting Timer"
    $timer.Start()    
}

function global:GetLocalDateTime($time)
{
    $timeStr = $time.toString()
    $unixTime = $timeStr.subString(0, $timeStr.Length - 3)
    $epoch = New-Object -Type DateTime -ArgumentList 1970, 1, 1, 0, 0, 0, 0
    return $epoch.AddSeconds($unixTime).ToLocalTime()
}

function Main($outfolder, $hostname, $queue, $encryptionKey, $username)
{   
    # assignment to global variables 
    $global:logOutputFolder = $outfolder
    $global:msmqHost = $hostname
    $global:queueName = $queue
    $global:encryptionKey = $encryptionKey
    $global:username = $username
    
    Write-Host "Folder : $logOutputFolder; Host : $msmqHost; Q: $queueName"
    # Kick start timer 
    SetupTimer 
}

# Parameter 
# Execute main powershell module 
Main $outfolder $hostname $myQueue $encryptionKey $username

