#!/bin/bash
# Matriky MZA Helper — one-click installer
# Usage: curl -sL https://g.book.cz/install-mza.sh | bash

set -e

EXT_DIR="$HOME/mza-helper-extension"
ZIP_URL="https://g.book.cz/mza-helper-extension.zip"

echo "=== Matriky MZA Helper ==="
echo ""

# Download and extract
echo "Stahuji rozšíření..."
curl -sL "$ZIP_URL" -o /tmp/mza-helper-extension.zip
rm -rf "$EXT_DIR"
mkdir -p "$EXT_DIR"
unzip -qo /tmp/mza-helper-extension.zip -d "$EXT_DIR"
rm /tmp/mza-helper-extension.zip

echo "Rozbaleno do: $EXT_DIR"
echo ""

# Open Chrome extensions page
echo "Otevírám Chrome..."
if [[ "$OSTYPE" == "darwin"* ]]; then
    open "chrome://extensions/"
elif command -v xdg-open &>/dev/null; then
    xdg-open "chrome://extensions/" 2>/dev/null || google-chrome "chrome://extensions/" 2>/dev/null
elif command -v google-chrome &>/dev/null; then
    google-chrome "chrome://extensions/"
fi

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║  Teď v Chrome proveďte 3 kroky:              ║"
echo "║                                              ║"
echo "║  1. Zapněte 'Developer mode' (vpravo nahoře) ║"
echo "║  2. Klikněte 'Load unpacked'                 ║"
echo "║  3. Vyberte složku: $EXT_DIR"
echo "║                                              ║"
echo "║  Hotovo! Rozšíření se spustí automaticky.    ║"
echo "╚══════════════════════════════════════════════╝"
