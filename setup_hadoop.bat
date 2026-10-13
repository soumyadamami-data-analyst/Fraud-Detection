@echo off
echo Downloading winutils for Hadoop 3.3.5...

powershell -Command "& {[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe' -OutFile 'C:\hadoop\bin\winutils.exe'}"
powershell -Command "& {[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll' -OutFile 'C:\hadoop\bin\hadoop.dll'}"

echo.
echo Done! Files downloaded to C:\hadoop\bin\
dir C:\hadoop\bin\
pause