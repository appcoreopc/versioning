#
# Summary : Read messages from a specific queue if it is older than (x) days.
# Peek messages in a queue to validate age
# If older, read and pop it off the queue using message selector 
# Feature v1.1b - Array server to pull messages, use jms's correlationId as filename,
# Example command
# Reading from a queue with a 5 minute wait time
# .\dlq.ps1 -outfolder "k:\\log" -hostname @("activemq:tcp://w12dvipfmom04.ldstatdv.net") -myqueue "asbhome" -encryptionKey 1234567890123456 -username si557912 -messageage 1

# Parameters
param(
    [Parameter(Mandatory=$true)]
    [string]$outfolder,
    [Parameter(Mandatory=$true)]
    [string[]]$hostname,
    [Parameter(Mandatory=$true)]
    [string]$myQueue,
    [Parameter(Mandatory=$true)]
    [string]$encryptionKey,
    [Parameter(Mandatory=$true)]
    [string]$username,
    [int]$messageAge
    )

Add-Type -Path Apache.NMS.dll

$global:queueName = ""
$global:timer = New-Object System.Timers.Timer(3000) # setting up timer for 3 second delay.
$global:runCount = 0
$global:msmqHost;
$global:maxAgeLimit = 1  # age of message in minutes
$global:connected = $false
$global:logOutputFolder = ""
$global:encryptionKey;
$global:username;
$global:defaultFilename = "unknown"
$global:qMaxRetry = 10
$global:qCurrentRetry = 0
$global:revisitQueue = $false;

function global:WriteMessage($queueMessage)
{
   FindCorrelationId($queueMessage)
}

function global:FindCorrelationId($queueMessage)
{
        $correlationId = $queueMessage.CorrelationId
        
        if ([string]::IsNullOrEmpty($queueMessage.CorrelationId))
        {
            $correlationId = $global:defaultFilename
        }
        LogMessageToFile $logOutputFolder $correlationId.Trim() $queueMessage
}

function global:LogMessageToFile([String] $path, [String]$fileName, $content)
{
    try {
        $fileToWrite = "$path\$fileName.log"
        $jSonContent = GetJsonMessageContent $content
        [System.IO.File]::AppendAllText($fileToWrite, $jSonContent)
    }
    catch
    {
        Write-Host "File write exception : " $_.Exception.Message -ForegroundColor red
    }
}

function global:GetJsonMessageContent($content)
{   
    $brokerPathData = "null"
    if (![string]::IsNullOrEmpty($content.brokerPath) -eq $true)
    {
        $brokerPathData = $content.brokerPath;
    }

    $persistentData = "NON-PERSISTENT"
    if ($content.Persistent -eq $true)
    {
        $persistentData = 'PERSISTENT';
    }
	
    $jsonContent = "{
        commandId : '$($content.commandId)',
        deliveryMode : '$persistentData',
        redelivered : '$($content.NMSRedelivered)',
        responseRequired :' $($content.responseRequired)',
        userId :' $($content.userId)',
        brokerPath :'$brokerPathData',
        ProducerId : '$($content.ProducerId)',
        Destination : '$($content.Destination)',
        TransactionId : '$($content.ActiveMQTextMessage.TransactionId)',
        OriginalDestination : '$($content.OriginalDestination)',
        MessageId  : '$($content.MessageId)',
        OriginalTransactionId : '$($content.OriginalTransactionId)',
        GroupID : '$($content.GroupID)',
        GroupSequence : '$($content.GroupSequence)',
        CorrelationId : '$($content.CorrelationId)',
        Persistent : '$($content.Persistent)',
        Expiration : '$($content.Expiration)',
        Priority : '$($content.Priority)',
        ReplyTo : '$($content.ReplyTo)',
        Timestamp : '$($content.Timestamp)',
        Type : '$($content.Type)',
        DataStructure : '$($content.DataStructure)',
        TargetConsumerId : '$($content.TargetConsumerId)',
        Compressed : '$($content.Compressed)',
        RedeliveryCounter : '$($content.RedeliveryCounter)',
        RecievedByDFBridge : '$($content.RecievedByDFBridge)',
        Droppable : '$($content.Droppable)',
        Cluster : '$($content.Cluster)',
        BrokerInTime : '$($content.BrokerInTime)',
        BrokerOutTime : '$($content.BrokerOutTime)',
        JMSXGroupFirstForConsumer : '$($content.JMSXGroupFirstForConsumer)',
        Text : '$($content.Text)',
        Authorization : '$($content.Properties.GetString("Authorization"))',
        Content_Type : '$($content.Properties.GetString("Content_Type"))',
        MULE_CORRELATION_ID : '$($content.Properties.GetString("MULE_CORRELATION_ID"))',
        MULE_ENCODING : '$($content.Properties.GetString("MULE_ENCODING"))',
        MULE_ENDPOINT : '$($content.Properties.GetString("MULE_ENDPOINT"))',
        MULE_MESSAGE_ID : '$($content.Properties.GetString("MULE_MESSAGE_ID"))',
        MULE_ORIGINATING_ENDPOINT : '$($content.Properties.GetString("MULE_ORIGINATING_ENDPOINT"))',
        MULE_ROOT_MESSAGE_ID : '$($content.Properties.GetString("MULE_ROOT_MESSAGE_ID"))',
        MULE_SESSION : '$($content.Properties.GetString("MULE_SESSION"))',
        event_type : '$($content.Properties.GetString("event_type"))'
    }"

    return $jsonContent
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

function global:CleanUp()
{
    Write-Host "Cleaning up resources used."
    $timer.Stop
}

function global:GetActiveQueueMessage($activeMqHostUrl)
{
    $hostsPort = @(61616, 61617)
    #$hostsPort = @(61616)
    #$hostsPort = @(61617)

    foreach ($hostTarget in $activeMqHostUrl)
    {
        foreach ($port in $hostsPort)
        {
            try { 
                GetQueueMessage($hostTarget + ":" + $port);

            }
            catch { 
                Write-Host "Core module error : $_.Exception.Message."
            }
        }
    }
}

function global:GetQueueMessage($activeMqHostUrl)
{
    $receiveMessageCount = 0
    $firstOccurenceAgeExceedMessageId = ""
    
    Write-Host "Connecting to the following activemq : $activeMqHostUrl" -ForegroundColor Cyan

    try  {

            # Create connection
            $connection = CreateConnection $activeMqHostUrl
            # Important!!!
            $connection.Start()

            $session = $connection.CreateSession([Apache.NMS.AcknowledgementMode]::Transactional)
            $target = [Apache.NMS.Util.SessionUtil]::GetDestination($session, "queue://$queueName")
            Write-Host "Establishing session to Queue :  $target  ---   $target.IsQueue " -ForegroundColor DarkCyan
            # Peek and remove message from the queue 
            PeekMessageQueue $queueName $session $target
    }
    catch {
        Write-Host "GetQueueMessage module error : $_.Exception.Message."
    }
    finally {
        # Important!!! : Otherwise connection gets locked
        Write-Host "Closing connection." -ForegroundColor Yellow
        $session.Close()
        $connection.Close()
        $global:RevisitQueue
    }
}

function global:GetQueryTime($minutesEarlier)
{
    $dTime = [System.DateTime]::UtcNow.AddMinutes(-$minutesEarlier)
    $firsTime = New-Object -Type DateTime -ArgumentList 1970, 1, 1, 0, 0, 0, 0
    $timespan = $dTime - $firsTime
    return $timespan.TotalMilliseconds;
}

function global:GetQueryTimeDays($daysEarlier)
{
    $dTime = [System.DateTime]::UtcNow.AddDays(-$daysEarlier)
    $firsTime = New-Object -Type DateTime -ArgumentList 1970, 1, 1, 0, 0, 0, 0
    $timespan = $dTime - $firsTime
    return $timespan.TotalMilliseconds;
}

function global:PeekMessageQueue($queueName, $session, $target)
{
    $count = 0;
    $queryTime = global:GetQueryTimeDays $global:maxAgeLimit
    $queryTime = [Math]::Floor($queryTime)
    
    try 
    {

    $targetQueue = $session.GetQueue($queueName)
    $query = "JMSTimestamp < $queryTime"
    Write-Host $query 
    $queueBrowser = $session.CreateBrowser($targetQueue, $query)
    #$queueBrowser = $session.CreateBrowser($targetQueue)
    $messages = $queueBrowser.GetEnumerator()

    Write-Host "Peeking message : $targetQueue" -ForegroundColor Yellow

    while ($messages.MoveNext())
    {
                $currentMessage = $messages.Current
                #Write-Host $currentMessage
                #Write-Host $currentMessage.Timestamp
                
                if ([string]::IsNullOrEmpty($currentMessage.MessageId) -ne $true) 
                { 
                        #Write-Host $currentMessage
                        
                        $messageTimestamp = GetLocalDateTime $currentMessage.Timestamp
                        $currentDate = $(Get-Date)
                        $duration = ($currentDate - $messageTimestamp).TotalMinutes
                        $durationDay = ($currentDate - $messageTimestamp).TotalDays

                        #$msgDuration = [Math]::Floor($duration)
                        $msgDuration = [Math]::Floor($durationDay)

                        Write-Host "MID:" $currentMessage.MessageId "CID:" $currentMessage.CorrelationId "Msg:[$messageTimestamp]: $msgDuration vs $maxAgeLimit " -ForegroundColor White

                        if ([int]$msgDuration -ge [int]$maxAgeLimit)
                        {
                            Write-Host "Attempting to Purge"

                            #$queueSelector = "JMSCorrelationID='47efd180-ba83-11e6-8774-005056bf2b05'" #-works
                            #$queueSelector = "JMSMessageID='ID:W12DVIPFMOM01-49320-1481955776780-150:1:1:1:1'" -works
                            # Truncating the text string based on last ":", otherwise you 
                            # will not be able to retrieve by MessageId = weirdness
                            
                            $msgId = [Convert]::ToString($($currentMessage.MessageId))
                            $msgLength = $msgId.LastIndexOf(":")
                            $finalId = $msgId.Substring(0, $msgLength)
                            $queueSelector = "JMSMessageID='$finalId'"

                            #$queueSelector = "JMSCorrelationID='$($currentMessage.MessageId)'" #-works
                            #Write-Host $queueSelector

                            $consumer =  $session.CreateConsumer($target, $queueSelector)
                            $msgReceived = $consumer.Receive(3000)
                            #Write-Host $msgReceived -ForegroundColor DarkGray

                            if ($msgReceived -ne $null) 
                            {
                                    Write-Host "Archiving $($msgReceived.MessageId) CorrelationId : [$($msgReceived.CorrelationId)] -selector- $queueSelector" -ForegroundColor Green
                                    Write-Host $msgReceived.CorrelationId -ForegroundColor DarkMagenta
                                    #Write-Host $msgReceived -ForegroundColor Green     
                                    WriteMessage $msgReceived
                            }
                            else 
                            {
                                Write-Host "Revisit queue operation set!"
                                $global:revisitQueue = $true
                            }
                        }
                        $count = $count  + 1
                }

            #break
        }
            $global:RevisitQueue
      }
    catch {
        Write-Host "PeekMessageQueue module error : $_.Exception.Message."
    }
    finally {
            Write-Host "Final code block execution"
            $queueBrowser.Close()
            if ($consumer -ne $null)
            {
                $consumer.Close()
                $session.Commit()
            }
            RevisitQueue
    }
    return $count;
} 

function global:RevisitQueue
{
      if ($global:revisitQueue -eq $true -and $global:qCurrentRetry -lt $global:qMaxRetry)
            {
                $global:revisitQueue = $false
                $global:qCurrentRetry += 1
                Write-Host "There are some messages in the queue. Setting up task count [$global:qCurrentRetry] to review queue"
                $global:timer.Interval = 9000
                $global:timer.Start();
            }
            else 
            {
                Write-Host "Halting timer services"
                CleanUp
            }

}

function global:CreateConnection($targetConnectionUrl)
{
    $targetConnectionUrl = $targetConnectionUrl + "?jms.optimizeAcknowledge=true"
    Write-Host "Preparing connectivity info to $targetConnectionUrl at : $((Get-Date).ToString())"
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

    try {
        $connection = $factory.CreateConnection($username, $password)
        Write-Host "Creating connection object : $connection " -ForegroundColor Green
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

    # Write-Host "Restarting Timer"
    $timer.Start()    
}

function global:GetLocalDateTime($time)
{
    $epoch = New-Object -Type DateTime -ArgumentList 1970, 1, 1, 0, 0, 0, 0
    $targetDate = $epoch.AddMilliseconds($time).ToLocalTime()
    return $targetDate
}

function Main($outfolder, $hostname, $queue, $encryptionKey, $username, $messageAge)
{
    # assignment to global variables
    $global:logOutputFolder = $outfolder
    $global:msmqHost = $hostname
    $global:queueName = $queue
    $global:encryptionKey = $encryptionKey
    $global:username = $username

    if ($messageAge -ne 0)
    {
        $global:maxAgeLimit = $messageAge
    }
    Write-Host "--------------------------------DLQ Reader Configurations 1.1d---------------------------------"
    Write-Host "Folder : $logOutputFolder; Host: $msmqHost; Q: $queueName; Message time in Q (MINUTES): $maxAgeLimit" -ForegroundColor Cyan
    Write-Host "-----------------------------------------------------------------------------------------------"

    SetupTimer 
}

# Parameter
# Execute main powershell module
Main $outfolder $hostname $myQueue $encryptionKey $username $messageAge
