@echo off
REM Geoprocessing only - batch script for Windows
REM Usage: geoprocess.bat [source_gdb_path] [aoi_boundary_path] [target_srid]

setlocal

REM Set default paths if not provided
set SOURCE_GDB=%1
if "%SOURCE_GDB%"=="" set SOURCE_GDB=data\staging.gdb

set AOI_BOUNDARY=%2
if "%AOI_BOUNDARY%"=="" set AOI_BOUNDARY=data\connections\municipality_boundary.shp

set TARGET_SRID=%3
if "%TARGET_SRID%"=="" set TARGET_SRID=3006

echo 🔄 Running geoprocessing...
echo   Source GDB: %SOURCE_GDB%
echo   AOI Boundary: %AOI_BOUNDARY%
echo   Target SRID: %TARGET_SRID%
echo.

python scripts\run_task.py geoprocess --source-gdb "%SOURCE_GDB%" --aoi-boundary "%AOI_BOUNDARY%" --target-srid %TARGET_SRID%

if %ERRORLEVEL% neq 0 (
    echo ❌ Geoprocessing failed with error code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
)

echo ✅ Geoprocessing completed successfully
endlocal
