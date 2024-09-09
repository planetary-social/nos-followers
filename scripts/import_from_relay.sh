#!/bin/bash

# Requires nak and moreutils (pee)

if [ -z "$1" ]; then
  echo "Usage: $0 <relay_url> [seconds_to_wait]"
  exit 1
fi

relay_url=$1
created_at_file="$relay_url-checkpoint.txt"
wait_seconds=${2:-2}
counter=0

record_created_at() {
  while read -r json; do
    created_at=$(jq -r '.created_at' <<< "$json" 2>/dev/null)
    formatted_date=$(jq -r '.created_at | strftime("%Y-%m-%d %H:%M:%S")' <<< "$json" 2>/dev/null)

    counter=$((counter + 1))
    echo -ne "\rSaving checkpoint: $created_at ($formatted_date) - Items so far: $counter" >&2
    tput el >&2
    echo "$created_at" > "$created_at_file"
  done
}

filter_events() {
  jq -cr 'select(
    (.tags | length > 1)
    or
    (.tags[0][0] != "p" or .tags[0][1] != "0497384b57b43c107a778870462901bf68e0e8583b32e2816563543c059784a4")
  )'
}


# Infinite loop
while true; do
  if [ -f "$created_at_file" ]; then
    initial_date=$(cat "$created_at_file")
    until_option="--until $initial_date"
  else
    until_option=""
  fi

  # Tee before jq filter to record all dates, even skipped entries
  nak req -k 3 $until_option --paginate --paginate-interval ${wait_seconds}s "$relay_url" \
  | filter_events \
  | tee >(record_created_at) \
  | nc localhost 3001

  echo "Command failed or completed, waiting for $wait_seconds seconds before retrying..." >&2
  sleep $wait_seconds
done
