@echo off
REM RetiMesh launcher for Windows
REM - Creates .venv on first run
REM - Installs / updates dependencies only when requirements.txt changes
REM - Runs retimesh.py, forwarding any arguments

setlocal EnableDelayedExpansion

REM Move to the script's directory so the script works no matter where it's invoked from
cd /d "%~dp0"

set "VENV_DIR=.venv"
set "REQ_FILE=requirements.txt"
set "HASH_FILE=%VENV_DIR%\.req.hash"
set "APP=retimesh.py"

REM ── 1. Locate a suitable Python interpreter ─────────────────────────────────
set "PYTHON="
where py >nul 2>&1
if %errorlevel%==0 (
    REM Prefer the launcher; ask for 3.x explicitly
    py -3 -c "import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)" >nul 2>&1
    if !errorlevel!==0 set "PYTHON=py -3"
)
if not defined PYTHON (
    where python >nul 2>&1
    if !errorlevel!==0 (
        python -c "import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)" >nul 2>&1
        if !errorlevel!==0 set "PYTHON=python"
    )
)
if not defined PYTHON (
    echo Error: Python 3.10+ is required but was not found in PATH.
    echo Install it from https://www.python.org/downloads/  ^(make sure to check "Add Python to PATH"^).
    exit /b 1
)
echo [run] Using interpreter: %PYTHON%
%PYTHON% --version

REM ── 2. Create the virtual environment if it doesn't exist ───────────────────
if not exist "%VENV_DIR%\Scripts\activate.bat" (
    echo [run] Creating virtual environment in %VENV_DIR% ...
    %PYTHON% -m venv "%VENV_DIR%"
    if errorlevel 1 (
        echo Error: 'python -m venv' failed.
        exit /b 1
    )
)

REM ── 3. Activate the virtualenv ──────────────────────────────────────────────
call "%VENV_DIR%\Scripts\activate.bat"
if errorlevel 1 (
    echo Error: failed to activate virtual environment.
    exit /b 1
)

REM ── 4. Install dependencies only when requirements.txt changed ──────────────
REM Use certutil to compute a SHA256 hash (ships with all modern Windows)
set "NEW_HASH="
for /f "skip=1 tokens=*" %%H in ('certutil -hashfile "%REQ_FILE%" SHA256 ^| findstr /v ":"') do (
    if not defined NEW_HASH set "NEW_HASH=%%H"
)
set "NEW_HASH=%NEW_HASH: =%"

set "OLD_HASH="
if exist "%HASH_FILE%" set /p OLD_HASH=<"%HASH_FILE%"

if not "%NEW_HASH%"=="%OLD_HASH%" (
    echo [run] Installing/updating dependencies from %REQ_FILE% ...
    python -m pip install --upgrade pip
    python -m pip install -r "%REQ_FILE%"
    if errorlevel 1 (
        echo Error: pip install failed.
        exit /b 1
    )
    > "%HASH_FILE%" echo %NEW_HASH%
) else (
    echo [run] Dependencies up to date.
)

REM ── 5. Launch the app, forwarding all CLI arguments ─────────────────────────
echo [run] Starting %APP% ...
python "%APP%" %*
endlocal
