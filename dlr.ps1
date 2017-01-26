#
# A powershell script to read from any ActiveMq provider
# $MyCredential=New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList "admin", (Get-Content $File | ConvertTo-SecureString -Key $key)
#
# Summary : Read messages from a specific queue if it is older than (x) days.
# Peek messages in a queue to validate age
# If older, read and pop it off the queue.
# 
# Feature v0.3 - Array server to pull messages, use jms's correlationId as filename, 
# Example command 
# Reading from a queue with a 5 minute wait time 
# .\dlq.ps1 -outfolder "k:\\log" -hostname "activemq:tcp://localhost" -myqueue "asbhomequote" -encryptionKey 1234567890123456 -username admin -messageAgeInMinutes 5

# Please remember to set Preference to a read/write credential for reading/writting to a queue.
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
    [int]$messageAgeInMinutes
    )

Add-Type -Path Apache.NMS.dll

$global:queueName = ""
$global:timer = New-Object System.Timers.Timer(5000)
$global:runCount = 0
$global:msmqHost;
$global:maxAgeDayLimit = 60 * 24  # age of message in day
$global:connected = $false
$global:logOutputFolder = "" 
$global:encryptionKey;
$global:username;
$global:defaultFilename = "unknown" 

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
        Write-Host $correlationId.Trim()
        LogMessageToFile $logOutputFolder $correlationId.Trim() $queueMessage
}

#Custom Message Property
#Authorization
#MULE_CORRELATION_ID

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
    $jsonContent = "{
        commandId : '$($content.commandId)',
        responseRequired :' $($content.responseRequired)',
        ProducerId : '$($content.ActiveMQTextMessage.ProducerId)', 
        Destination : '$($content.ActiveMQTextMessage.Destination)',
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
        Timestamp : '$($content.Timestamp),
        Type : '$($content.Type)', 
        MarshalledProperties : '$($content.MarshalledProperties)',
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
        MULE_CORRELATION_ID : '$($content.Properties.GetString("MULE_CORRELATION_ID"))'
    }"

    return $jsonContent
}

################################################################################
# 
################################################################################
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
    foreach ($hostTarget in $activeMqHostUrl)
    {
        Write-Host "Hosts :[$hostTarget]"
        foreach ($port in $hostsPort)
        {
            GetQueueMessage($hostTarget + ":" + $port);    
        }
    }    
}

function global:GetQueueMessage($activeMqHostUrl)
{
    Write-Host "Connecting to the following activemq : $activeMqHostUrl" -ForegroundColor Cyan
    # Create connection
    $connection = CreateConnection $activeMqHostUrl
    
    try  { 
            $session = $connection.CreateSession()
            $target = [Apache.NMS.Util.SessionUtil]::GetDestination($session, "queue://$queueName")
            Write-Host "Establishing session to Queue :  $target  ---   $target.IsQueue " -ForegroundColor DarkCyan

            # creating message queue consumer. 
            # using the Listener - event listener might not be suitable 
            # as we only logs expired messages in the queue. 

            $consumer =  $session.CreateConsumer($target)

            $connection.Start()            
            Write-Host "Successfully started a connection to server." -ForegroundColor Green

            # Get old enuff messages from the queue 
            $msgCount = PeekMessageQueue $queueName 

            if ($msgCount -gt 0) 
            {
                Write-Host "Trying to receive/archive messages"
                $receiveMessageCount = 0
                while (($imsg = $consumer.Receive([TimeSpan]::FromMilliseconds(2000))) -ne $null) 
                {
                    $receiveMessageCount = $receiveMessageCount + 1
                    Write-Host "Popping messages from queue. [$receiveMessageCount]" 
                    #Write-Host $imsg

                    if ($imsg -ne $null) 
                    {
                        WriteMessage($imsg)
                    }

                    if ( $receiveMessageCount -eq $msgCount )
                    {
                        Write-Host "Messages to be taken out of the queue reached." -ForegroundColor Blue
                        break;
                    }
                }
            }

            Write-Host "Closing connection." -ForegroundColor Yellow
            $connection.Close()
            #RestartTimer # Disable this feature for now.
    }
    catch {
        Write-Host "Core module error : $_.Exception.Message."
    }
    finally {
        CleanUp
    }
}

function global:PeekMessageQueue($queueName)
{
    $count = 0;
    $targetQueue = $session.GetQueue($queueName)
    $queueBrowser = $session.CreateBrowser($targetQueue)
    $messages = $queueBrowser.GetEnumerator()

    Write-Host "Peeking message using component : $targetQueue" -ForegroundColor Yellow
    while ($messages.MoveNext())
    {
           $currentMessage = $messages.Current
           $messageTimestamp = GetLocalDateTime $currentMessage.Timestamp
   
           $messageDays = $(Get-Date).Subtract($messageTimestamp).Minutes
           Write-Host "Message diff age is : $messageDays" -ForegroundColor DarkYellow

           # kicks out, if message is less than a day old #     
           if ($messageDays -lt $maxAgeDayLimit)
           {
             break; 
           }
           $count = $count  + 1
    }

    $queueBrowser.Close()
    Write-Host "Total number of records to pop from queue are : $count"
    return $count;
}

function global:CreateConnection($targetConnectionUrl)
{
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
        Write-Host "Creating connection object : $connection" -ForegroundColor Green
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

function Main($outfolder, $hostname, $queue, $encryptionKey, $username, $messageAgeInMinutes)
{   
    # assignment to global variables 
    $global:logOutputFolder = $outfolder
    $global:msmqHost = $hostname
    $global:queueName = $queue
    $global:encryptionKey = $encryptionKey
    $global:username = $username

    if ($messageAgeInMinutes -ne 0)
    {
        $global:maxAgeDayLimit = $messageAgeInMinutes
    }
    Write-Host "-----------------------------------------------------------------------------------------------"
    Write-Host "Folder : $logOutputFolder; Host: $msmqHost; Q: $queueName; Message time in Q (minutes): $maxAgeDayLimit" -ForegroundColor Cyan
    Write-Host "-----------------------------------------------------------------------------------------------"
    # Kick start timer 
    SetupTimer 
}

# Parameter 
# Execute main powershell module 
Main $outfolder $hostname $myQueue $encryptionKey $username $messageAgeInMinutes
