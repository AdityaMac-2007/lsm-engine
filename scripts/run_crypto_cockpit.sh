#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ADDR="${1:-127.0.0.1:8080}"
DATA_DIR="${2:-$ROOT_DIR/tsdb_crypto_data}"
TMP_DIR="$(mktemp -d /tmp/lsm-crypto-XXXXXX)"

export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="/tmp/lsm-target"

echo "Building LSM-TSDB release server..."
cargo build --release --bin tsdb_server --manifest-path "$ROOT_DIR/Cargo.toml"

"/tmp/lsm-target/release/tsdb_server" --addr "$ADDR" --dir "$DATA_DIR" &
SERVER_PID=$!

CLEANED=0
cleanup() {
  [ "$CLEANED" -eq 1 ] && return
  CLEANED=1
  echo ""
  echo "Shutting down LSM-TSDB (PID $SERVER_PID)..."
  kill "$SERVER_PID" 2>/dev/null || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT
trap 'exit 0' INT TERM

# Wait for HTTP server readiness (up to 5s)
READY=0
for _ in $(seq 1 25); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "[error] tsdb_server exited early (is $ADDR already in use?)."
    exit 1
  fi
  if curl -fsS --max-time 1 "http://${ADDR}/api/stats" >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 0.2
done

if [ "$READY" -ne 1 ]; then
  echo "[error] tsdb_server did not respond on http://${ADDR}"
  exit 1
fi

echo "=========================================================="
echo "  LSM-TSDB Market Cockpit: http://$ADDR"
echo "  Streaming live BTC / ETH / SOL ticks every 1s (Ctrl+C)"
echo "=========================================================="

fetch_pair() {
  local CB_PAIR="$1" METRIC="$2" KR_PAIR="$3" BN_PAIR="$4" OUT_FILE="$5"
  local RAW="" PARSED=""

  RAW=$(curl -fsS --max-time 2 -H "User-Agent: LSM-TSDB/1.0" "https://api.exchange.coinbase.com/products/${CB_PAIR}/ticker" 2>/dev/null || true)
  if [ -n "$RAW" ]; then
    PARSED=$(printf '%s' "$RAW" | python3 -c 'import sys,json; d=json.load(sys.stdin); p=float(d["price"]); b=float(d.get("bid",p)); a=float(d.get("ask",p)); sp=((a-b)/p)*10000 if p>0 else 0.0; print(f"{p:.4f} {sp:.4f} coinbase")' 2>/dev/null || true)
  fi

  if [ -z "$PARSED" ]; then
    RAW=$(curl -fsS --max-time 2 -H "User-Agent: LSM-TSDB/1.0" "https://api.kraken.com/0/public/Ticker?pair=${KR_PAIR}" 2>/dev/null || true)
    if [ -n "$RAW" ]; then
      PARSED=$(printf '%s' "$RAW" | python3 -c 'import sys,json; d=json.load(sys.stdin); e=next(iter(d["result"].values())); p=float(e["c"][0]); a=float(e["a"][0]); b=float(e["b"][0]); sp=((a-b)/p)*10000 if p>0 else 0.0; print(f"{p:.4f} {sp:.4f} kraken")' 2>/dev/null || true)
    fi
  fi

  if [ -z "$PARSED" ]; then
    RAW=$(curl -fsS --max-time 2 -H "User-Agent: LSM-TSDB/1.0" "https://api.binance.com/api/v3/ticker/bookTicker?symbol=${BN_PAIR}" 2>/dev/null || true)
    if [ -n "$RAW" ]; then
      PARSED=$(printf '%s' "$RAW" | python3 -c 'import sys,json; d=json.load(sys.stdin); b=float(d["bidPrice"]); a=float(d["askPrice"]); p=(a+b)/2.0; sp=((a-b)/p)*10000 if p>0 else 0.0; print(f"{p:.4f} {sp:.4f} binance")' 2>/dev/null || true)
    fi
  fi

  if [ -n "$PARSED" ]; then
    printf '%s %s\n' "$METRIC" "$PARSED" > "$OUT_FILE"
  fi
}

TICK=0
while true; do
  LOOP_START=$(python3 -c 'import time; print(time.time())')
  TS_MS=$(python3 -c 'import time; print(int(time.time()*1000))')

  rm -f "$TMP_DIR"/*.out
  fetch_pair "BTC-USD" "btc.usd" "XBTUSD" "BTCUSDT" "$TMP_DIR/btc.out" &
  P1=$!
  fetch_pair "ETH-USD" "eth.usd" "ETHUSD" "ETHUSDT" "$TMP_DIR/eth.out" &
  P2=$!
  fetch_pair "SOL-USD" "sol.usd" "SOLUSD" "SOLUSDT" "$TMP_DIR/sol.out" &
  P3=$!
  wait "$P1" "$P2" "$P3" 2>/dev/null || true

  PAYLOAD=""
  SUMMARY=""
  for F in "$TMP_DIR/btc.out" "$TMP_DIR/eth.out" "$TMP_DIR/sol.out"; do
    if [ -f "$F" ]; then
      read -r METRIC PRICE SPREAD SRC < "$F"
      PAYLOAD+="${METRIC} ${PRICE} ${TS_MS}"$'\n'
      SUMMARY+="${METRIC}=\$${PRICE} (${SRC}) | "
      if [ "$METRIC" = "btc.usd" ]; then
        PAYLOAD+="btc.spread_bps ${SPREAD} ${TS_MS}"$'\n'
      fi
    fi
  done

  if [ -n "$PAYLOAD" ]; then
    RESP=$(printf '%s' "$PAYLOAD" | curl -fsS --max-time 2 -X POST "http://${ADDR}/api/ingest" --data-binary @- 2>/dev/null || echo "retry")
    TICK=$((TICK + 1))
    echo "[tick #${TICK}] ${SUMMARY}-> ${RESP}"
  else
    echo "[warn] Could not fetch exchange tickers this cycle, retrying..."
  fi

  # Keep steady 1.0s cadence by subtracting elapsed fetch time
  SLEEP_S=$(python3 -c "import time; rem = 1.0 - (time.time() - ${LOOP_START}); print(f'{max(0.05, rem):.3f}')")
  sleep "$SLEEP_S"
done
