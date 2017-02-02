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
# .\dlq.ps1 -outfolder "k:\\log" -hostname @("activemq:tcp://w12dvipfmom04.ldstatdv.net") -myqueue "asbhome" -encryptionKey 1234567890123456 -username si557912 -messageage 1

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
    [int]$messageAge
    )

Add-Type -Path Apache.NMS.dll

$global:queueName = ""
$global:timer = New-Object System.Timers.Timer(1000)
$global:runCount = 0
$global:msmqHost;
$global:maxAgeLimit = 1  # age of message in day
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
        Write-Host "Finding CorrelationId"
        Write-Host $correlationId

        if ([string]::IsNullOrEmpty($queueMessage.CorrelationId))
        {
            $correlationId = $global:defaultFilename
        }
        Write-Host $correlationId.Trim()
        LogMessageToFile $logOutputFolder $correlationId.Trim() $queueMessage
}

function global:LogMessageToFile([String] $path, [String]$fileName, $content)
{
    Write-Host "target path"
    Write-Host $path
    Write-Host "filename"
    Write-Host $fileName

    try {
        $fileToWrite = "$path\$fileName.log"

        Write-Host ""
        Write-Host $fileToWrite

        $jSonContent = GetJsonMessageContent $content
        Write-Host "-----------------------------------------"
        Write-Host $jSonContent
        Write-Host "-----------------------------------------"

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
        deliveryMode : '$($content.DeliveryMode)',
        redelivered : '$($content.redelivered)',
        responseRequired :' $($content.responseRequired)',
        userId :' $($content.userId)',
        brokerPath :' $($content.brokerPath)',
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
    #$hostsPort = @(61616, 61617)
    $hostsPort = @(61616)

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
    $receiveMessageCount = 0
    $firstOccurenceAgeExceedMessageId = ""
    
    Write-Host "Connecting to the following activemq : $activeMqHostUrl" -ForegroundColor Cyan
    # Create connection
    $connection = CreateConnection $activeMqHostUrl
    # Important!!!
    $connection.Start()

    try  {
            #$session = $connection.CreateSession([Apache.NMS.AcknowledgementMode]::Transactional)
            $session = $connection.CreateSession()
            $target = [Apache.NMS.Util.SessionUtil]::GetDestination($session, "queue://$queueName")

            if ($queueName.Contains("*"))
            {
                $renameQueue = $queueName.Replace("*", "_")
                $tempQueue = $renameQueue + "_temp" 
                Write-Host "Final queue name $tempQueue"
            }
            else 
            {
                $tempQueue = $queueName + "_temp"
            }

            $targetTmp = [Apache.NMS.Util.SessionUtil]::GetDestination($session, "queue://$tempQueue")
            Write-Host "Establishing session to Queue :  $target  ---   $target.IsQueue " -ForegroundColor DarkCyan

            # creating message queue consumer.
            # using the Listener - event listener might not be suitable
            # as we only logs expired messages in the queue.
            
            #$consumer =  $session.CreateConsumer($target)
            $tempMessageWriter =  $session.CreateProducer($targetTmp)
            
            Write-Host "Successfully started a connection to server." -ForegroundColor Green
            
            # Get old enuff messages from the queue
            $msgCount = PeekMessageQueue $queueName $session $target

            # while (($imsg = $consumer.Receive([TimeSpan]::FromMilliseconds(2000))) -ne $null)
            # {
            #          $receiveMessageCount = $receiveMessageCount + 1
            #          $messageTimestamp = GetLocalDateTime $imsg.Timestamp
            #          $currentDate = $(Get-Date)
            #          $duration = ($currentDate - $messageTimestamp).TotalMinutes
            #          $durationDay = ($currentDate - $messageTimestamp).TotalDays

            #          Write-Host $imsg.CorrelationId "message timestamp [$messageTimestamp] diff is : $duration (minutes) - $durationDay (days)" -ForegroundColor DarkYellow

            #          if ($duration -lt $maxAgeLimit)
            #          {
            #             RewriteBackToQueue $session $tempMessageWriter $imsg
            #          }
            #          else
            #          {
            #              if ($imsg -ne $null)
            #             {
            #                  WriteMessage($imsg)
            #             }

            #             $receiveMessageCount = $receiveMessageCount  + 1
            #          }
            # }

            #RewriteMessageToDLQ $session $targetTmp $target
            #Write-Host "Purging temp queue destination" -ForegroundColor Yellow
            #[Apache.NMS.Util.SessionUtil]::DeleteDestination($session, "queue://$tempQueue")

    }
    catch {
        Write-Host "Core module error : $_.Exception.Message."
    }
    finally {
        # Important!!! : Otherwise connection gets locked
        Write-Host "Finally closing connection." -ForegroundColor Yellow
        $session.Close()
        $connection.Close()
        CleanUp
    }
}

function global:PeekMessageQueue($queueName, $session, $target)
{
    $count = 0;
    $targetQueue = $session.GetQueue($queueName)
    $queueBrowser = $session.CreateBrowser($targetQueue)
    $messages = $queueBrowser.GetEnumerator()
    
    Write-Host "Peeking message for queue : $targetQueue" -ForegroundColor Yellow

    while ($messages.MoveNext())
    {
           $currentMessage = $messages.Current
           #Write-Host $currentMessage
           $messageTimestamp = GetLocalDateTime $currentMessage.Timestamp
           $currentDate = $(Get-Date)
           $duration = ($currentDate - $messageTimestamp).TotalMinutes
           $durationDay = ($currentDate - $messageTimestamp).TotalDays

           if ($duration -ge $maxAgeLimit)
           {
               #$queueSelector = "JMSCorrelationID='bbbbbbbbbbbbbbbbbbbbbbbbbbbb'" works
               #$queueSelector2 = "JMSMessageID='ID:W12DVIPFMOM01-49320-1481955776780-150:1:1:1:1'" works
               
               $msgId = [Convert]::ToString($($currentMessage.MessageId))
               $finalId = $msgId.Substring(0, $msgId.Length - 2)
               $queueSelector = "JMSMessageID='$finalId'"
               Write-Host $queueSelector -ForegroundColor Cyan

               Write-Host $queueSelector    

               $consumer =  $session.CreateConsumer($target, $queueSelector)
               $msgReceived = $consumer.Receive(2000)
               #$msgReceived = $consumer.Receive(2000) # works!

               if ($msgReceived -ne $null) {
                    Write-Host $msgReceived -ForegroundColor Green 
                    WriteMessage $msgReceived
               }
               break;
           }

           $count = $count  + 1
           Write-Host $currentMessage.CorrelationId "message timestamp [$messageTimestamp] diff is : $duration (minutes) - $durationDay (days)" -ForegroundColor DarkYellow
    }
    #$session.Commit()
    $queueBrowser.Close()

    if ($consumer -ne $null)
    {
        $consumer.Close()
    }
    
    #Write-Host "Total number of records to pop from queue are : $count"
    return $count;
}

function global:RewriteMessageToDLQ($session, $source, $target)
{
      Write-Host "Transfering queues in temp to DLQ."

      $sourceQueue =  $session.CreateConsumer($source)
      $targetQueue =  $session.CreateProducer($target)

      while (($imsg = $sourceQueue.Receive([TimeSpan]::FromMilliseconds(2000))) -ne $null)
      {
          RewriteBackToQueue $session $targetQueue $imsg
      }
}

function global:RewriteBackToQueue($session, $producer, $message)
{
     if ($message.Text -ne $null)
     {
        $queueMsgStructure = $session.CreateTextMessage($message.Text)
     }

     if ($message.deliveryMode -ne $null)
     {
        $queueMsgStructure.deliveryMode = $message.deliveryMode
     }

     if ($message.redelivered -ne $null) {
        $queueMsgStructure.redelivered = $message.redelivered
     }

     if ($message.responseRequired -ne $null) {
        $queueMsgStructure.responseRequired = $message.responseRequired
     }

     if ($message.userId -ne $null) {
        $queueMsgStructure.userId = $message.userId
     }

     if ($message.brokerPath -ne $null)
     {
     $queueMsgStructure.brokerPath = $message.brokerPath
     }

     if ($message.ProducerId -ne $null) {
            $queueMsgStructure.ProducerId = $message.ProducerId
     }

     if ($message.Destination -ne $null) {
        $queueMsgStructure.Destination = $message.Destination
     }

     if ($message.TransactionId -ne $queueMsgStructure.TransactionId)
     {
        $queueMsgStructure.TransactionId = $message.TransactionId
     }

     if ( $message.OriginalDestination -ne $null)
     {
        $queueMsgStructure.OriginalDestination = $message.OriginalDestination
     }

     if ($message.OriginalTransactionId -ne $null) {
        $queueMsgStructure.OriginalTransactionId = $message.OriginalTransactionId
     }

     if ($message.GroupID -ne $null)
     {
        $queueMsgStructure.GroupID = $message.GroupID
     }

     if ($message.GroupSequence -ne $null)
     {
        $queueMsgStructure.GroupSequence = $message.GroupSequence
     }

     if ($message.CorrelationId -ne $null) {
        $queueMsgStructure.CorrelationId = $message.CorrelationId
     }

     if ($message.Persistent -ne $null) {
        $queueMsgStructure.Persistent = $message.Persistent
     }

     if ($message.Expiration -ne $null)
      {
        $queueMsgStructure.Expiration = $message.Expiration
      }

      if ($message.Priority -ne $null) {
        $queueMsgStructure.Priority = $message.Priority
      }

      if ($message.ReplyTo -ne $null)
      {
        $queueMsgStructure.ReplyTo = $message.ReplyTo
      }

      if ($message.Timestamp -ne $null) {
        $queueMsgStructure.Timestamp = $message.Timestamp
      }

      if ($message.MarshalledProperties -ne $null)
      {
            $queueMsgStructure.MarshalledProperties = $message.MarshalledProperties
      }

     if ($message.Type -ne $null)
     {
        $queueMsgStructure.Type = $message.Type
     }

     if ($message.DataStructure -ne $null)
     {
        $queueMsgStructure.DataStructure = $message.DataStructure
     }

     if ($message.TargetConsumerId -ne $null)
     {
        $queueMsgStructure.TargetConsumerId = $message.TargetConsumerId
     }

    if ($message.Compressed -ne $null)
     {
        $queueMsgStructure.Compressed = $message.Compressed
     }

     if ($message.RedeliveryCounter -ne $null)
     {
        $queueMsgStructure.RedeliveryCounter = $message.RedeliveryCounter
     }
     if ($message.RecievedByDFBridge -ne $null)
     {
        $queueMsgStructure.Compressed = $message.RecievedByDFBridge
     }
     if ($message.Droppable -ne $null)
     {
        $queueMsgStructure.Droppable = $message.Droppable
     }
     if ($message.Cluster -ne $null)
     {
        $queueMsgStructure.Cluster = $message.Cluster
     }
     if ($message.BrokerInTime -ne $null)
     {
        $queueMsgStructure.BrokerInTime = $message.BrokerInTime
     }
     if ($message.BrokerOutTime -ne $null)
     {
        $queueMsgStructure.BrokerOutTime = $message.BrokerOutTime
     }
     if ($message.JMSXGroupFirstForConsumer -ne $null)
     {
        $queueMsgStructure.JMSXGroupFirstForConsumer = $message.JMSXGroupFirstForConsumer
     }
     if ($message.Authorization -ne $null)
     {
        $queueMsgStructure.Authorization = $message.Authorization
     }

     if ($message.Content_Type -ne $null)
     {
        $queueMsgStructure.Content_Type = $message.Content_Type
     }

      $queueMsgStructure.Properties.SetString("MULE_CORRELATION_ID", $message.Properties.GetString("MULE_CORRELATION_ID"))
      $queueMsgStructure.Properties.SetString("MULE_ENCODING", $message.Properties.GetString("MULE_ENCODING"))
      $queueMsgStructure.Properties.SetString("MULE_ENDPOINT", $message.Properties.GetString("MULE_ENDPOINT"))
      $queueMsgStructure.Properties.SetString("MULE_MESSAGE_ID", $message.Properties.GetString("MULE_MESSAGE_ID"))
      $queueMsgStructure.Properties.SetString("MULE_ORIGINATING_ENDPOINT", $message.Properties.GetString("MULE_ORIGINATING_ENDPOINT"))
      $queueMsgStructure.Properties.SetString("MULE_ROOT_MESSAGE_ID", $message.Properties.GetString("MULE_ROOT_MESSAGE_ID"))
      $queueMsgStructure.Properties.SetString("MULE_SESSION", $message.Properties.GetString("MULE_SESSION"))
    
      $producer.Send($queueMsgStructure)

        # deliveryMode : '$($content.DeliveryMode)',
        # redelivered : '$($content.redelivered)',
        # responseRequired :' $($content.responseRequired)',
        # userId :' $($content.userId)',
        # brokerPath :' $($content.brokerPath)',
        # ProducerId : '$($content.ActiveMQTextMessage.ProducerId)',
        # Destination : '$($content.ActiveMQTextMessage.Destination)',
        # TransactionId : '$($content.ActiveMQTextMessage.TransactionId)',
        # OriginalDestination : '$($content.OriginalDestination)',
        # MessageId  : '$($content.MessageId)',
        # OriginalTransactionId : '$($content.OriginalTransactionId)',
        # GroupID : '$($content.GroupID)',
        # GroupSequence : '$($content.GroupSequence)',
        # CorrelationId : '$($content.CorrelationId)',
        # Persistent : '$($content.Persistent)',
        # Expiration : '$($content.Expiration)',
        # Priority : '$($content.Priority)',
        # ReplyTo : '$($content.ReplyTo)',
        # Timestamp : '$($content.Timestamp)',
        # MarshalledProperties : '$($content.MarshalledProperties)',
        # Type : '$($content.Type)',
        # DataStructure : '$($content.DataStructure)',
        # TargetConsumerId : '$($content.TargetConsumerId)',
        # Compressed : '$($content.Compressed)',
        # RedeliveryCounter : '$($content.RedeliveryCounter)',
        # RecievedByDFBridge : '$($content.RecievedByDFBridge)',
        # Droppable : '$($content.Droppable)',
        # Cluster : '$($content.Cluster)',
        # BrokerInTime : '$($content.BrokerInTime)',
        # BrokerOutTime : '$($content.BrokerOutTime)',
        # JMSXGroupFirstForConsumer : '$($content.JMSXGroupFirstForConsumer)',
        # Text : '$($content.Text)',
        # Authorization : '$($content.Properties.GetString("Authorization"))',
        # Content_Type : '$($content.Properties.GetString("Content_Type"))',
        # MULE_CORRELATION_ID : '$($content.Properties.GetString("MULE_CORRELATION_ID"))',
        # MULE_ENCODING : '$($content.Properties.GetString("MULE_ENCODING"))',
        # MULE_ENDPOINT : '$($content.Properties.GetString("MULE_ENDPOINT"))',
        # MULE_MESSAGE_ID : '$($content.Properties.GetString("MULE_MESSAGE_ID"))',
        # MULE_ORIGINATING_ENDPOINT : '$($content.Properties.GetString("MULE_ORIGINATING_ENDPOINT"))',
        # MULE_ROOT_MESSAGE_ID : '$($content.Properties.GetString("MULE_ROOT_MESSAGE_ID"))',
        # MULE_SESSION : '$($content.Properties.GetString("MULE_SESSION"))',
        # event_type : '$($content.Properties.GetString("event_type"))'

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
    Write-Host "--------------------------------DEBUG VERSION OF THE Q-Reader----------------------------------"
    Write-Host "Folder : $logOutputFolder; Host: $msmqHost; Q: $queueName; Message time in Q (MINUTES): $maxAgeLimit" -ForegroundColor Cyan
    Write-Host "-----------------------------------------------------------------------------------------------"
    # Kick start timer
    SetupTimer
}

# Parameter
# Execute main powershell module
Main $outfolder $hostname $myQueue $encryptionKey $username $messageAge
