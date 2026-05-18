#!/bin/bash

processes=$1

if [ -z "$processes" ] || [ "$processes" -lt 1 ]; then
  echo "please indicate a number of processes of at least one"
  exit 0
fi

babelport=34000
base_server_port=35000

skip_babel_port=34002
skip_server_port=35002

get_babel_port() {
  local i=$1
  local port=$((babelport + i))

  if [ "$port" -ge "$skip_babel_port" ]; then
    port=$((port + 1))
  fi

  echo "$port"
}

get_server_port() {
  local i=$1
  local port=$((base_server_port + i))

  if [ "$port" -ge "$skip_server_port" ]; then
    port=$((port + 1))
  fi

  echo "$port"
}

membership="localhost:$(get_babel_port 0)"

read -p "------------- Press enter start. After starting, press enter to kill all servers --------------------"

i=1
while [ "$i" -lt "$processes" ]; do
    membership="${membership},localhost:$(get_babel_port "$i")"
    i=$((i + 1))
done

i=0
while [ "$i" -lt "$processes" ]; do
  current_babel_port=$(get_babel_port "$i")
  current_server_port=$(get_server_port "$i")

  java \
    -DlogFilename=logs/node$current_babel_port \
    -cp target/DistAlg.jar Main \
    babel.address=127.0.0.1 \
    babel.port=$current_babel_port \
    server_port=$current_server_port \
    initial_membership=$membership \
    2>&1 | sed "s/^/[$current_babel_port] /" | tee -a logs/node$current_babel_port.log &

  echo "launched process on SMR port $current_babel_port, server port $current_server_port"

  sleep 1
  i=$((i + 1))
done

sleep 2
read -p "------------- Press enter to kill servers. --------------------"

kill $(ps aux | grep 'DistAlg.jar' | awk '{print $2}')

echo "All processes done!"

