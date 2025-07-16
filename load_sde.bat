@echo off
REM Load to SDE only - batch script for Windows
REM Usage: load_sde.bat [source_gdb_path] [sde_connection_path]

setlocal

REM Set default paths if not provided
set SOURCE_GDB=%1
if "%SOURCE_GDB%"=="" set SOURCE_GDB=data\staging.gdb

set SDE_CONNECTION=%2
if "%SDE_CONNECTION%"=="" set SDE_CONNECTION=data\connections\prod.sde

echo 🚚 Loading data to SDE...
echo   Source GDB: %SOURCE_GDB%
echo   SDE Connection: %SDE_CONNECTION%
echo.

python scripts\run_task.py sde --source-gdb "%SOURCE_GDB%" --sde-connection "%SDE_CONNECTION%"

if %ERRORLEVEL% neq 0 (
    echo ❌ SDE loading failed with error code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
)

echo ✅ SDE loading completed successfully
endlocal
