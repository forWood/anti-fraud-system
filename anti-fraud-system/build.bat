@echo off
set JAVA_HOME=D:\tools\java\jdk8\zulu8.84.0.15-ca-jdk8.0.442-win_x64
set PATH=%JAVA_HOME%\bin;D:\tools\maven\apache-maven-3.9.16\bin;%PATH%
cd /d e:\workspace\codebuddy\fxsb\anti-fraud-system
echo JAVA_HOME=%JAVA_HOME%
echo.
call mvn clean package -DskipTests -T 4
exit /b %ERRORLEVEL%
