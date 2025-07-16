@echo off
REM Staging only - batch script for Windows
REM Usage: stage.bat [--force-download] [--no-reset-gdb]

setlocal

set ARGS=

REM Parse arguments
:parse_args
if "%1"=="" goto run_staging
if "%1"=="--force-download" (
    set ARGS=%ARGS% --force-download
    shift
    goto parse_args
)
if "%1"=="--no-reset-gdb" (
    set ARGS=%ARGS% --no-reset-gdb
    shift
    goto parse_args
)
shift
goto parse_args

:run_staging
echo 📦 Running staging task...
echo   Arguments: %ARGS%
echo.

python scripts\run_task.py stage %ARGS%

if %ERRORLEVEL% neq 0 (
    echo ❌ Staging failed with error code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
)

echo ✅ Staging completed successfully
endlocal
