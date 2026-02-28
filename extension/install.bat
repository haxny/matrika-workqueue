@echo off
chcp 65001 >nul 2>&1
title Matriky MZA Helper - Instalace

echo === Matriky MZA Helper ===
echo.

echo Stahuji rozsireni...
curl -sL https://g.book.cz/mza-helper-extension.zip -o "%TEMP%\mza-helper-extension.zip"
if errorlevel 1 (
    echo CHYBA: Nelze stahnout. Zkontrolujte pripojeni k internetu.
    pause
    exit /b 1
)

echo Rozbaluji...
if exist "%USERPROFILE%\mza-helper-extension" rmdir /s /q "%USERPROFILE%\mza-helper-extension"
mkdir "%USERPROFILE%\mza-helper-extension"
tar -xf "%TEMP%\mza-helper-extension.zip" -C "%USERPROFILE%\mza-helper-extension"
del "%TEMP%\mza-helper-extension.zip"

echo.
echo ========================================
echo.
echo   HOTOVO! Rozsireni stazeno.
echo.
echo   Ted v Chrome proved'te tyto kroky:
echo.
echo   1. Do adresniho radku zadejte:
echo      chrome://extensions
echo.
echo   2. Zapnete "Developer mode" (vpravo nahore)
echo.
echo   3. Kliknete "Load unpacked"
echo.
echo   4. V okne, ktere se otevre, je jiz
echo      vybrana spravna slozka - staci
echo      kliknout "Vybrat slozku"
echo.
echo   Rozsireni se spusti automaticky.
echo   Muzete toto okno zavrit.
echo.
echo ========================================
echo.

:: Open the extension folder so it's ready in Explorer
explorer "%USERPROFILE%\mza-helper-extension"

pause
