@echo off
set "JAVA_HOME=D:\tools\java\jdk8\zulu8.84.0.15-ca-jdk8.0.442-win_x64"
set "PATH=%JAVA_HOME%\bin;D:\tools\maven\apache-maven-3.9.16\bin;%PATH%"
cd /d "e:\workspace\codebuddy\fxsb\anti-fraud-system"
echo === BUILDING ALL MODULES ===
call "D:\tools\maven\apache-maven-3.9.16\bin\mvn.cmd" clean package -DskipTests > mvn_full_build.log 2>&1
set EXIT_CODE=%ERRORLEVEL%
echo Exit code: %EXIT_CODE%
if %EXIT_CODE% EQU 0 (echo === BUILD SUCCESS ===) else (echo === BUILD FAILED ===)
exit /b %EXIT_CODE%
