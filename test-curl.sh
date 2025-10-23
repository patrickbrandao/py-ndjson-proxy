#!/bin/sh

# (rode algum worker)

# Health check
curl -v http://localhost:8771/ping;


# Teste streaming
curl -X POST http://localhost:8771/test19282 \
    -H "Content-Type: application/json" \
    -d '{"message": "teste"}' \
    --no-buffer;


