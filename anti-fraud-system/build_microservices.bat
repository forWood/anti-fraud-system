@echo off
set JAVA_HOME=D:\tools\java\jdk8\zulu8.84.0.15-ca-jdk8.0.442-win_x64
set PATH=%JAVA_HOME%\bin;D:\tools\maven\apache-maven-3.9.16\bin;%PATH%
cd /d e:\workspace\codebuddy\fxsb\anti-fraud-system
call mvn package -DskipTests -pl anti-fraud-microservices/transaction-monitor,anti-fraud-microservices/alert-management,anti-fraud-microservices/case-management,anti-fraud-microservices/report-generation -am > mvn_microservices.log 2>&1
exit /b %ERRORLEVEL%
