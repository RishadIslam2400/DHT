#!/bin/bash

# cl.sh
#
# A script for uploading and running romulus applications on the Sunlab

set -e # Halt the script on any error

REMOTE_BUILD_DIR="~/DHT/build"
export SCREENDIR=$HOME/.screen
mkdir -p $SCREENDIR
chmod 700 $SCREENDIR

# Print usage information
usage() {
	cat <<EOF

cl.sh — tool for building & running binary executables on the Sunlab
Usage: ./cl.sh <command> [arguments]
Commands:
    install-deps            Setup SSH keys and verify remote hosts.
    init-config             Generate the cluster configuration file (config.txt).
    run <exe> <ops> <threads> <range>
                            Run the compiled binary.
                            <exe> : Name of the target in build/
                            <ops> : Number of operations
                            <threads>: Number of threads
                            <range> : Key range
    connect                 Open a screen session connected to all nodes.
    reset <proc_name>       Kill a specific process name on all nodes.
    reset-all               Kill all user processes on all nodes.
    do-all <cmd>            Run a raw shell command on all nodes.
EOF
}

# Generate Property File
function generate_cluster_config() {
    OUTPUT_FILE="config.txt"
    DEFAULT_PORT=1895
    
    echo "Generating cluster configuration: $OUTPUT_FILE"
    
    > "$OUTPUT_FILE"
    
    for i in "${!MACHINES[@]}"; do
        host="${MACHINES[$i]}"
        full_host="${host}.${DOMAIN}"
        resolved_ip=$(getent hosts "$full_host" | awk '{ print $1 }' | head -n 1)
        
        if [ -z "$resolved_ip" ]; then
            resolved_ip="$host"
        fi
        
        # Write to file: ID IP PORT
        echo "$i $resolved_ip $DEFAULT_PORT" >> "$OUTPUT_FILE"
    done
    
    echo "Successfully created $OUTPUT_FILE with ${#MACHINES[@]} nodes."
}

# SSH into MACHINES once, to fix known_hosts
function cl_first_connect {
	echo "Performing one-time connection to Sunlab MACHINES, to get known_hosts right"
	for machine in ${MACHINES[@]}; do
		echo "$USER@$machine.$DOMAIN"
		ssh -o StrictHostKeyChecking=no $USER@$machine.$DOMAIN echo "Connected"
	done
}

# Append the default configuration of a screenrc to the given file
function make_screen {
	echo 'startup_message off' >>$1
	echo 'defscrollback 10000' >>$1
	echo 'autodetach on' >>$1
	echo 'escape ^jj' >>$1
	echo 'defflow off' >>$1
	echo 'hardstatus alwayslastline "%w"' >>$1
}

#  Configure the set of remote MACHINES
function cl_install_deps() {
	echo "Setup initial connection"

	# Check if the agent is running; if not, start it
	if [ -z "$SSH_AUTH_SOCK" ]; then
		echo "Starting ssh-agent..."
		eval "$(ssh-agent -s)"
	fi
	ssh-add ~/.ssh/id_ed25519
	cl_first_connect
}

# SEND and RUN a binary on the remote MACHINES
# $1: Exe, $2: Ops, $3: Threads, $4: Key Range
function cl_run() {
	# check if file exists
	EXE_NAME=$(basename "$1")
	NUM_OPS=$2
    NUM_THREADS=$3
    KEY_RANGE=$4
	
	if [[ -z "$1" || -z "$2" || -z "$3" || -z "$4" ]]; then
        echo "Usage: ./cl.sh run <exe> <ops> <threads> <range>"
        exit 1
    fi

	generate_cluster_config

	echo "Uploading config.txt to all nodes (Destination: ${REMOTE_BUILD_DIR})..."
    for m in ${MACHINES[*]}; do
        scp "config.txt" "${USER}@${m}.${DOMAIN}:${REMOTE_BUILD_DIR}/" &
    done
    wait
	rm -rf logs
	mkdir logs
	# Set up a screen script for running the program on all MACHINES
	tmp_screen="$(mktemp)" || exit 1
	make_screen "$tmp_screen"

	echo "Launching $EXE_NAME from $REMOTE_BUILD_DIR on remote nodes..."

	for i in "${!MACHINES[@]}"; do
		host="${MACHINES[$i]}"
		#ARGS="your args" # modify it
		
		# COMMAND: cd to build dir -> run binary -> pass config location
        # Note: We assume config.txt is in HOME (~/), so we pass "../config.txt" or "~/config.txt"
		CMD="cd ${REMOTE_BUILD_DIR} && ./${EXE_NAME} config.txt ${i} ${NUM_OPS} ${NUM_THREADS} ${KEY_RANGE}"
		echo "Node $i ($host): $CMD"
		cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh ${USER}@${host}.${DOMAIN} ${CMD}; bash
logfile logs/log_${i}.txt
log on
EOF
	done

	screen -c "$tmp_screen"
	rm "$tmp_screen"
}

# Connect to remote nodes (e.g., for debugging)
function cl_connect() {
	last_valid_index=$((${#MACHINES[@]} - 1)) # The 0-indexed number of nodes

	# Set up a screen script for connecting
	tmp_screen="$(mktemp)" || exit 1
	make_screen $tmp_screen
	for i in $(seq 0 ${last_valid_index}); do
		echo "screen -t node${i} ssh ${USER}@${MACHINES[$i]}.${DOMAIN}" >>${tmp_screen}
	done
	screen -c $tmp_screen
	rm $tmp_screen
}

function do_all {
	for i in "${!MACHINES[@]}"; do
		ssh ${USER}@${MACHINES[$i]}.${DOMAIN} "$1" &
	done
	wait
}

function reset-all() {
	last_valid_index=$((${#MACHINES[@]} - 1)) # The 0-indexed number of nodes
	for i in $(seq 0 ${last_valid_index}); do
		ssh ${USER}@${MACHINES[$i]}.${DOMAIN} "killall -9 -u $USER" &
	done
	wait
	echo "Nodes have been reset."
}

function reset() {
	last_valid_index=$((${#MACHINES[@]} - 1)) # The 0-indexed number of nodes
	for i in $(seq 0 ${last_valid_index}); do
		ssh ${USER}@${MACHINES[$i]}.${DOMAIN} "pkill $1 || true" &
	done
	wait
	echo "Nodes have been reset."
}


# Get the important stuff out of the command-line args
cmd=$1   # The requested command
count=$# # The number of command-line args
# Navigate the the project root directory
cd $(git rev-parse --show-toplevel)
# Load the config right away
for file in config/*.conf; do
	if [ -f $file ]; then
		source $file
	fi
done

if [[ "$cmd" == "install-deps" && "$count" -eq 1 ]]; then
	cl_install_deps
elif [[ "$cmd" == "init-config" ]]; then
    generate_cluster_config
elif [[ "$cmd" == "run" && "$count" -eq 5 ]]; then
	cl_run "$2" "$3" "$4" "$5"
elif [[ "$cmd" == "connect" && "$count" -eq 1 ]]; then
	cl_connect
elif [[ "$cmd" == "reset" && "$count" -eq 2 ]]; then
	reset $2
elif [[ "$cmd" == "reset-all" && "$count" -eq 1 ]]; then
	reset-all
elif [[ "$cmd" == "do-all" && "$count" -eq 2 ]]; then
	do_all "$2"
else
	usage
fi