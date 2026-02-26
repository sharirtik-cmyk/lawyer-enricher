#!/bin/bash
# ── Legal Scout — Startup Script ──
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🔧 Installing dependencies..."
pip install -r requirements.txt --break-system-packages -q

echo ""
echo "════════════════════════════════════════"
echo "  ⚖️  Legal Scout — Lawyer Enrichment System"
echo "════════════════════════════════════════"
echo ""

# Check for API key
if [ -z "$ANTHROPIC_API_KEY" ]; then
  echo "⚠️  WARNING: ANTHROPIC_API_KEY not set!"
  echo "   Website classification and search will fail."
  echo "   Set it with: export ANTHROPIC_API_KEY=your_key"
  echo ""
fi

echo "🚀 Starting server on http://localhost:5000"
echo "   Open your browser to: http://localhost:5000"
echo ""
echo "Press Ctrl+C to stop."
echo ""

cd backend
python app.py
