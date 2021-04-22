#!/bin/sh
echo "sleeping and waiting for kafka connect"
sleep 20s
echo "starting worker..."
node consumer.js