param(
    [Parameter(Mandatory=$true)]
    [string]$userLoginName,
    [Parameter(Mandatory=$true)]
    [string]$UserPassword,
    [Parameter(Mandatory=$true)]
    [string]$keyString
    )

### Encrypting #######
$File = "Password.txt"
[Byte[]] $key = [System.Text.Encoding]::UTF8.GetBytes($keyString.Trim())
Write-Host "User provided key length are : [$key.length]"
$Password = $UserPassword | ConvertTo-SecureString -AsPlainText -Force
$Password | ConvertFrom-SecureString -key $key | Out-File $File

### Test Decryption ###
$MyCredential=New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $userLoginName, (Get-Content $File | ConvertTo-SecureString -Key $key)
$username = $MyCredential.UserName.toString()
$password = $MyCredential.GetNetworkCredential().Password

Write-Host "Testing decryption: Username : $username, password is [$password]" 
