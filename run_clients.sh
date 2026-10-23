#!/usr/bin/env bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <numClients> <numThreads> <numNumbers>"
    echo "Example: $0 4 4 1000000"
    exit 1
fi

NUM_CLIENTS="$1"
NUM_THREADS="$2"
NUM_NUMBERS="$3"

if ! [[ "$NUM_CLIENTS" =~ ^[0-9]+$ ]] || ! [[ "$NUM_THREADS" =~ ^[0-9]+$ ]] || ! [[ "$NUM_NUMBERS" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] All arguments must be positive integers."
    exit 1
fi

if [ "$NUM_CLIENTS" -le 0 ] || [ "$NUM_THREADS" -le 0 ] || [ "$NUM_NUMBERS" -le 0 ]; then
    echo "[ERROR] All arguments must be greater than zero."
    exit 1
fi

if ! command -v runhaskell >/dev/null 2>&1; then
    echo "[ERROR] runhaskell not found. Install ghc or compile with make."
    exit 1
fi

if ! command -v ss >/dev/null 2>&1; then
    if ! command -v lsof >/dev/null 2>&1; then
        echo "[WARN] Could not check if server is listening on port 8081."
    fi
fi

if command -v ss >/dev/null 2>&1; then
    if ! ss -tln 2>/dev/null | grep -q ":8081"; then
        echo "[WARN] Nothing seems to be listening on port 8081. Make sure the server is running."
    fi
elif command -v lsof >/dev/null 2>&1; then
    if ! lsof -i :8081 >/dev/null 2>&1; then
        echo "[WARN] Nothing seems to be listening on port 8081. Make sure the server is running."
    fi
fi

echo "[INFO] Starting $NUM_CLIENTS clients via runhaskell -threaded | threads=$NUM_THREADS | numbers=$NUM_NUMBERS"
echo "[INFO] Logs will be written to client<ID>.log and server.log"

PIDS=()
for i in $(seq 1 "$NUM_CLIENTS"); do
    echo "[LAUNCH] Client $i -> runhaskell -threaded client.hs $i $NUM_THREADS $NUM_NUMBERS"
    runhaskell -threaded client.hs "$i" "$NUM_THREADS" "$NUM_NUMBERS" &
    PIDS+=($!)
done

echo "[WAIT] Waiting for all clients to finish..."
for pid in "${PIDS[@]}"; do
    wait "$pid"
done

echo "[DONE] All $NUM_CLIENTS clients finished."
