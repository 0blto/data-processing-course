$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root

New-Item -ItemType Directory -Force -Path "$root\secrets", "$root\conf\ssh" | Out-Null

$pwFile = "$root\secrets\gpdb_password"
if (-not (Test-Path $pwFile)) {
  Copy-Item "$root\secrets\gpdb_password.example" $pwFile
  Write-Host "Создан \secrets\gpdb_password из примера (пароль gpadmin: gparray)."
}

$key = "$root\conf\ssh\id_rsa"
if (-not (Test-Path $key)) {
  ssh-keygen -t rsa -b 4096 -f $key -N '""' -q
  Copy-Item "$root\conf\ssh\id_rsa.pub" "$root\conf\ssh\authorized_keys" -Force
  Write-Host "Сгенерированы SSH-ключи в conf\ssh (нужны для woblerr/greenplum multi-node)."
}

Write-Host "Подготовка завершена"
