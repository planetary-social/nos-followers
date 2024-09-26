#!/bin/bash

followee1=89ef92b9ebe6dc1e4ea398f6477f227e95429627b0a33dc89b640e137b256be5
followee2=89ef92b9ebe6dc1e4ea398f6477f227e95429627b0a33dc89b640e137b256be4

get_random_followee() {
  if (( RANDOM % 2 == 0 )); then
    echo "$followee1"
  else
    echo "$followee2"
  fi
}

while :; do
  for ((i=1; i<=10; i++)); do
    followee=$(get_random_followee)

    nak event -k 3 -t p="$followee" --sec $(nostrkeytool --gen) ws://localhost:7777
  done
done
