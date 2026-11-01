@echo off
rem ------------------------------------------------------------
rem Batch script to kill existing service processes on required ports
rem and start all microservices sequentially with delays.
rem ------------------------------------------------------------

setlocal enabledelayedexpansion

rem List of ports used by the services
set ports=8081 8082 8083 8084 8088 8089

echo Killing any processes listening on the service ports...
for %%P in (%ports%) do (
    for /f "tokens=5" %%A in ('netstat -ano ^| findstr :%%P ^| findstr LISTENING') do (
        echo Stopping process ID %%A on port %%P
        taskkill /PID %%A /F >nul 2>&1
    )
)

rem Small pause to ensure ports are freed
timeout /t 2 /nobreak >nul

rem Function to start a service
:runService
set service=%1
set dir=%2
set port=%3

echo Starting %service% (port %port%)...
pushd %dir%
start "" cmd /c "mvn spring-boot:run"
popd
rem Wait a few seconds before next service to allow startup
timeout /t 5 /nobreak >nul
goto :eof

rem ------------------------------------------------------------
rem Start services in order
rem ------------------------------------------------------------
call :runService Account-service C:\project\Digital_Banking_System\Account-service 8081
call :runService Transaction-service C:\project\Digital_Banking_System\Transaction-service 8082
call :runService Payment-service C:\project\Digital_Banking_System\Payment-service 8083
call :runService Fraud-Detection-service C:\project\Digital_Banking_System\Fraud-Detection-service 8084
call :runService Notification-service C:\project\Digital_Banking_System\Notification-service 8088
call :runService api-gateway C:\project\Digital_Banking_System\api-gateway 8089

echo All services launched. Use the health-check script to verify.
pause

