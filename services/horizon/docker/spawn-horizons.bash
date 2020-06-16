#!/bin/bash

PARALLELISM=3

BASE_LEDGER=20064319
RANGE_WIDTH=10048


for I in `seq $PARALLELISM`; do
    docker-compose -f docker-compose.yml -f docker-compose.pubnet.yml run -d horizon db reingest range $(( BASE_LEDGER + RANGE_WIDTH * (I-1) )) $(( BASE_LEDGER + RANGE_WIDTH * I ))
done
