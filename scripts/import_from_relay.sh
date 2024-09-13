#!/bin/bash
#

# Requires nak

if [ -z "$1" ]; then
  echo "A tool to paginate events and store them for future restarts from the last position it could read."
  echo "It includes a filter to detect spam and can exponentially skip events if stuck at a certain timestamp."
  echo "Usage:"
  echo "  To send to followers via TCP port: $0 <relay_url> [seconds_to_wait] | nc localhost 3001"
  echo "  Or send to a specific relay:       $0 <relay_url> [seconds_to_wait] | nak event -qq relay.nos.social"

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

previous_p_value=""
last_p_value=""
filter_events() {
  while read -r event; do
    current_p_value=$(echo "$event" | jq -r '.tags[0][1]')

    # Skip if the current p value is the same as the last p value stored
    if [[ "$current_p_value" == "$last_p_value" ]]; then
      continue  # Probably spam, skip this event
    fi

    # Update the last seen p value
    previous_p_value="$last_p_value"
    last_p_value="$current_p_value"

    # Apply the filtering logic
    echo "$event" | jq -cr 'select(
      (.tags | length > 1) or
      (
        .tags[0][0] != "p" or
        .tags[0][1] != "0497384b57b43c107a778870462901bf68e0e8583b32e2816563543c059784a4" or
        .tags[0][1] != "4bc7982c4ee4078b2ada5340ae673f18d3b6a664b1f97e8d6799e6074cb5c39d"
      )
    )'
  done
}


# Infinite loop
decrease_secs=2
while true; do
  if [ -f "$created_at_file" ]; then
    current_date=$(cat "$created_at_file")
    until_option="--until $current_date"
  else
    until_option=""
  fi

  echo "Iteration starting" >&2

  nak req -k 3 $until_option --paginate --paginate-interval ${wait_seconds}s "$relay_url" | filter_events | tee >(record_created_at) >&1


  previous_date="$current_date"
  current_date=$(cat "$created_at_file")

  if [ "$current_date" == "$previous_date" ] && [ "$last_p_value" == "$previous_p_value" ]; then
    echo "Reducing time by $decrease_secs seconds" >&2

    current_date=$((current_date - decrease_secs))
    decrease_secs=$((decrease_secs * 2))
  else
    decrease_secs=2
  fi
done
