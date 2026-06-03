@echo off
setlocal enabledelayedexpansion

set "NoAppStart="
if /I "%1"=="-NoAppStart" set "NoAppStart=1"
if /I "%1"=="/NoAppStart" set "NoAppStart=1"

set "ProjectDir=%~dp0"
set "ProjectDir=%ProjectDir:~0,-1%"

echo Stopping any running chaturbate-dvr, ffmpeg, cloudflared processes...
taskkill /F /IM chaturbate-dvr.exe 2>nul
taskkill /F /IM ffmpeg.exe 2>nul
taskkill /F /IM cloudflared.exe 2>nul
echo   [OK] Done
echo.

echo ============================================
echo     MiniDelectableService -- Full Setup
echo ============================================
echo.

REM -- 1. Install FFmpeg via winget --
echo [1/7] Installing FFmpeg...
where ffmpeg >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    winget install Gyan.FFmpeg.Essentials --accept-package-agreements --accept-source-agreements

    for /f "tokens=2*" %%a in ('reg query "HKCU\Environment" /v Path 2^>nul') do set "UserPath=%%b"
    for /f "tokens=2*" %%a in ('reg query "HKLM\SYSTEM\CurrentControlSet\Control\Session Manager\Environment" /v Path 2^>nul') do set "MachinePath=%%b"
    set "PATH=%MachinePath%;%UserPath%"

    where ffmpeg >nul 2>nul
    if !ERRORLEVEL! NEQ 0 (
        set "WINGET_FOUND="
        for /f "delims=" %%f in ('dir /s /b "%LOCALAPPDATA%\Microsoft\WinGet\Packages\Gyan.FFmpeg.Essentials*\ffmpeg.exe" 2^>nul') do (
            if exist "%%f" set "WINGET_FOUND=%%~dpf"
        )
        if not defined WINGET_FOUND (
            if exist "%LOCALAPPDATA%\Microsoft\WinGet\Links\ffmpeg.exe" for %%f in ("%LOCALAPPDATA%\Microsoft\WinGet\Links\ffmpeg.exe") do set "WINGET_FOUND=%%~dpf"
        )
        if defined WINGET_FOUND (
            echo   Found ffmpeg at: !WINGET_FOUND!
            REM setx PATH replaced with reg add for GitHub Actions compatibility
            reg add "HKCU\Environment" /v Path /t REG_EXPAND_SZ /d "!UserPath!;!WINGET_FOUND!" /f >nul 2>&1
            set "PATH=!PATH!;!WINGET_FOUND!"
            REM Force refresh PATH for where command
            for /f "tokens=2*" %%a in ('reg query "HKCU\Environment" /v Path 2^>nul') do set "UserPath=%%b"
            for /f "tokens=2*" %%a in ('reg query "HKLM\SYSTEM\CurrentControlSet\Control\Session Manager\Environment" /v Path 2^>nul') do set "MachinePath=%%b"
            set "PATH=%MachinePath%;%UserPath%;!WINGET_FOUND!"
        )
    )

    where ffmpeg >nul 2>nul
    if !ERRORLEVEL! NEQ 0 (
        REM Last resort: check if binary exists and skip error
        set "WINGET_FOUND="
        for /f "delims=" %%f in ('dir /s /b "%LOCALAPPDATA%\Microsoft\WinGet\Packages\Gyan.FFmpeg.Essentials*\ffmpeg.exe" 2^>nul') do (
            if exist "%%f" set "WINGET_FOUND=%%~dpf"
        )
        if not defined WINGET_FOUND (
            if exist "%LOCALAPPDATA%\Microsoft\WinGet\Links\ffmpeg.exe" for %%f in ("%LOCALAPPDATA%\Microsoft\WinGet\Links\ffmpeg.exe") do set "WINGET_FOUND=%%~dpf"
        )
        if defined WINGET_FOUND (
            echo   [WARN] FFmpeg installed but 'where' can't find it - adding to PATH manually
            set "PATH=!PATH!;!WINGET_FOUND!"
            echo   [OK] FFmpeg available at !WINGET_FOUND!
        ) else (
            echo ERROR: FFmpeg could not be found after install
            exit /b 1
        )
    ) else (
        for /f "delims=" %%a in ('where ffmpeg') do echo   [OK] FFmpeg installed at %%a
    )
) else (
    for /f "delims=" %%a in ('where ffmpeg') do echo   [OK] FFmpeg already installed at %%a
)

REM -- 2. Ensure ffmpeg is in PATH --
echo [2/7] Ensuring ffmpeg is on PATH...
where ffmpeg >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    REM Try to find it manually if where fails
    set "FOUND_MANUALLY="
    for /f "delims=" %%f in ('dir /s /b "%LOCALAPPDATA%\Microsoft\WinGet\Packages\Gyan.FFmpeg.Essentials*\ffmpeg.exe" 2^>nul') do (
        if exist "%%f" (
            for %%d in ("%%f") do set "FOUND_MANUALLY=%%~dpd"
            set "PATH=!PATH!;!FOUND_MANUALLY!"
        )
    )
    if not defined FOUND_MANUALLY (
        if exist "%LOCALAPPDATA%\Microsoft\WinGet\Links\ffmpeg.exe" (
            for %%f in ("%LOCALAPPDATA%\Microsoft\WinGet\Links\ffmpeg.exe") do set "FOUND_MANUALLY=%%~dpf"
            set "PATH=!PATH!;!FOUND_MANUALLY!"
        )
    )
    if not defined FOUND_MANUALLY (
        echo ERROR: ffmpeg not found in PATH -- DVR will fail to mux/thumbnail
        exit /b 1
    )
    echo   [OK] Found and added ffmpeg to PATH: !FOUND_MANUALLY!
)

REM Get ffmpeg directory
set "ffmpegDir="
where ffmpeg >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    for /f "delims=" %%a in ('where ffmpeg') do set "ffmpegPath=%%a"
    for %%a in ("%ffmpegPath%") do set "ffmpegDir=%%~dpa"
    set "ffmpegDir=%ffmpegDir:~0,-1%"
) else (
    REM Use the manually found path
    set "ffmpegDir=%FOUND_MANUALLY%"
    set "ffmpegDir=%ffmpegDir:~0,-1%"
)

if defined ffmpegDir (
    for /f "tokens=2*" %%a in ('reg query "HKCU\Environment" /v Path 2^>nul') do set "UserPath=%%b"
    set "ALREADY_IN_PATH="
    echo !UserPath! | C:\Windows\System32\find.exe /I "!ffmpegDir!" >nul 2>&1
    if !ERRORLEVEL! EQU 0 set "ALREADY_IN_PATH=1"
    
    if not defined ALREADY_IN_PATH (
        REM setx PATH replaced with reg add for GitHub Actions compatibility
        reg add "HKCU\Environment" /v Path /t REG_EXPAND_SZ /d "!UserPath!;!ffmpegDir!" /f >nul 2>&1
        set "PATH=!PATH!;!ffmpegDir!"
        echo   [OK] Added !ffmpegDir! to user PATH
    ) else (
        echo   [OK] Already in PATH: !ffmpegDir!
    )
)

for /f "tokens=2*" %%a in ('reg query "HKLM\SYSTEM\CurrentControlSet\Control\Session Manager\Environment" /v Path 2^>nul') do set "MachinePath=%%b"
for /f "tokens=2*" %%a in ('reg query "HKCU\Environment" /v Path 2^>nul') do set "UserPath=%%b"
set "PATH=%MachinePath%;%UserPath%"

REM -- 3. Install cloudflared --
echo [3/7] Installing cloudflared...
where cloudflared >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    winget install Cloudflare.cloudflared --accept-package-agreements --accept-source-agreements
    for /f "tokens=2*" %%a in ('reg query "HKLM\SYSTEM\CurrentControlSet\Control\Session Manager\Environment" /v Path 2^>nul') do set "MachinePath=%%b"
    for /f "tokens=2*" %%a in ('reg query "HKCU\Environment" /v Path 2^>nul') do set "UserPath=%%b"
    set "PATH=%MachinePath%;%UserPath%"
    where cloudflared >nul 2>nul
    if !ERRORLEVEL! NEQ 0 (
        echo   [WARN] cloudflared install failed (tunnel won't be available)
    ) else (
        echo   [OK] cloudflared installed
    )
) else (
    echo   [OK] cloudflared already installed
)

REM -- 4. Install Go via winget --
echo [4/7] Installing Go...
where go >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    winget install GoLang.Go --accept-package-agreements --accept-source-agreements
    for /f "tokens=2*" %%a in ('reg query "HKLM\SYSTEM\CurrentControlSet\Control\Session Manager\Environment" /v Path 2^>nul') do set "MachinePath=%%b"
    for /f "tokens=2*" %%a in ('reg query "HKCU\Environment" /v Path 2^>nul') do set "UserPath=%%b"
    set "PATH=%MachinePath%;%UserPath%"
    where go >nul 2>nul
    if !ERRORLEVEL! NEQ 0 (
        echo ERROR: Go install failed
        exit /b 1
    ) else (
        echo   [OK] Go installed
    )
) else (
    echo   [OK] Go already installed
)

REM -- 5. Install Go dependencies --
echo [5/7] Installing Go dependencies...
cd /d "%ProjectDir%"
call go mod download
echo   [OK] Go modules downloaded

REM -- 6. Build Go binary --
echo [6/7] Building Go binary...
call go build -o chaturbate-dvr.exe .
echo   [OK] Build complete

REM -- 7. Install Node.js dependencies --
echo [7/7] Installing Node.js dependencies...
call npm install
echo   [OK] Node.js deps installed

REM -- Copy .env if missing --
if not exist "%ProjectDir%\.env" (
    copy "%ProjectDir%\.env.example" "%ProjectDir%\.env" >nul
    echo.>>"%ProjectDir%\.env"
    echo # Safety: don't delete local files after upload>>"%ProjectDir%\.env"
    echo DELETE_LOCAL_AFTER_UPLOAD=false>>"%ProjectDir%\.env"
    echo   [INFO] Created .env from .env.example -- edit it with your API keys!
)

echo.
echo ============================================
echo            [OK]  Setup complete!
echo ============================================
echo.

if not defined NoAppStart (
    echo Starting chaturbate-dvr in a new window...
    start "chaturbate-dvr" "%ProjectDir%\chaturbate-dvr.exe" --no-tunnel
    timeout /t 2 /nobreak >nul

    echo Starting Cloudflare tunnel in a new window...
    start "Cloudflare Tunnel" cmd /k "cloudflared tunnel --url http://localhost:8080 --protocol http2"

    echo.
    echo   DVR:     http://localhost:8080
    echo   Tunnel:  check the new window for the public URL
    echo.
    echo Close the windows when done.
    echo.
) else (
    echo Run '.\chaturbate-dvr.exe --no-tunnel' to start the app
    echo Then start the tunnel separately with: cloudflared tunnel --url http://localhost:8080
)

endlocal
