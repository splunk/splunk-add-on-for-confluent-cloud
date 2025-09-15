#!/bin/bash
# filepath: /workspaces/confluent-ta/confluent_addon_for_splunk/scripts/build-ui.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "Node.JS is not installed. Please install Node.JS to continue."
    exit 1
fi

echo "🔧 Installing UI dependencies..."
if [ "$CI" = "true" ]; then
    npm --prefix "$SCRIPT_DIR/../ui" ci
else
    npm --prefix "$SCRIPT_DIR/../ui" install
fi

echo "🔧 Building UI components with UCC..."
npm --prefix "$SCRIPT_DIR/../ui" run ucc-gen

echo "✅ UI build completed successfully"