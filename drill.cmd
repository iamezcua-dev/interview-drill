@echo off
rem drill launcher for Windows (cmd / PowerShell). The only prerequisite
rem is git: this script runs the drill CLI through uv, which downloads the
rem right Python and drill's dependencies on first use.
setlocal
set "DRILL_ROOT=%~dp0"
set "DRILL_ROOT=%DRILL_ROOT:~0,-1%"
call :find_uv
if defined UV goto run

echo drill needs uv, a small tool that installs the right Python and
echo drill's dependencies for you: https://docs.astral.sh/uv/
set "REPLY="
set /p "REPLY=Install uv now with its official installer? [y/N] "
if /i "%REPLY%"=="y" goto install
if /i "%REPLY%"=="yes" goto install
echo Install it yourself with:
echo   powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
exit /b 1

:install
rem Install to %USERPROFILE%\.local\bin without editing your PATH.
set "UV_NO_MODIFY_PATH=1"
powershell -NoProfile -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
call :find_uv
if defined UV goto run
echo uv was installed but can't be found; open a new terminal.
exit /b 1

:run
"%UV%" run --quiet --project "%DRILL_ROOT%" drill %*
exit /b %ERRORLEVEL%

:find_uv
set "UV="
for %%I in (uv.exe) do if not "%%~$PATH:I"=="" set "UV=%%~$PATH:I"
if not defined UV if exist "%USERPROFILE%\.local\bin\uv.exe" set "UV=%USERPROFILE%\.local\bin\uv.exe"
if not defined UV if exist "%USERPROFILE%\.cargo\bin\uv.exe" set "UV=%USERPROFILE%\.cargo\bin\uv.exe"
exit /b 0
