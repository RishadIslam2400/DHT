#!/bin/bash

# cl.sh
#
# A script for uploading and running romulus applications on the Sunlab

set -e # Halt the script on any error

# Print usage information
usage() {
	cat <<EOF

cl.sh — tool for building & running binary executables on the Sunlab
Usage: ./cl.sh <command> [arguments]
Commands:
	install-deps            Setup SSH keys and verify remote hosts.
	build-run <mode> <exe>  Compile and run a binary.
							<mode>: debug | release
	                        <exe> : Name of the target in build/
	connect                 Open a screen session connected to all nodes.
	reset					Kill a specific process name on all nodes.
	reset-all               Kill all user processes on all nodes.
	do-all <cmd>            Run a raw shell command on all nodes.
Examples:
	./cl.sh build-run debug my_app
	./cl.sh do-all "uptime"
...
EOF
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
# $1 : Relative path of exe
function cl_run() {
	# check if file exists
	EXE_NAME=$(basename "$1")
	if [[ ! -f "build/$1" ]]; then
		echo "Executable not found: $1"
		exit 1
	fi
	for m in ${MACHINES[*]}; do
		scp "build/$1" "${USER}@${m}.${DOMAIN}:${EXE_NAME}" &
	done
	wait
	rm -rf logs
	mkdir logs
	# Set up a screen script for running the program on all MACHINES
	tmp_screen="$(mktemp)" || exit 1
	make_screen "$tmp_screen"


	for i in "${!MACHINES[@]}"; do
		host="${MACHINES[$i]}"
		ARGS="your args" # modify it
		CMD="your binary" # modify it
		echo "$CMD"
		cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh ${USER}@${host}.${DOMAIN} ${CMD}; bash
logfile logs/log_${i}.txt
log on
EOF
	done

	screen -c "$tmp_screen"
	rm "$tmp_screen"
}

# $1 : Relative path of exe
function cl_debug() {
	EXE_NAME=$(basename "$1")
	if [[ ! -f "build/$1" ]]; then
		echo "Executable not found: $1"
		exit 1
	fi
	# Send the executable to all MACHINES
	for m in ${MACHINES[*]}; do
		scp "build/$1" "${USER}@${m}.${DOMAIN}:${EXE_NAME}" &
	done
	wait
	rm -rf gdb-logs
	mkdir gdb-logs

	# Set up a screen script for running the program on all MACHINES
	tmp_screen="$(mktemp)" || exit 1
	make_screen $tmp_screen

	gdb_cmd="$2"
	echo "Running gdb with command: $gdb_cmd"

	for i in "${!MACHINES[@]}"; do
		host="${MACHINES[$i]}"
		CMD="--hostname ${host} --node-id ${i} --leader-fixed ${ARGS}"
		if [[ $i -eq 0 && -n "$gdb_cmd" ]]; then
			cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh ${USER}@${host}.${DOMAIN} gdb -ex \"${gdb_cmd}\" -ex \"r\" --args ./${EXE_NAME} ${CMD}; bash
logfile gdb-logs/gdb_${i}.log
log on
EOF
		else
			cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh ${USER}@${host}.${DOMAIN} gdb -ex \"r\" --args ./${EXE_NAME} ${CMD}; bash
logfile gdb-logs/gdb_${i}.log
log on
EOF
		fi
	done
	screen -c $tmp_screen
	rm $tmp_screen
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
		ssh ${USER}@${MACHINES[$i]}.${DOMAIN} "sudo killall -9 -u $USER" &
	done
	wait
	echo "Nodes have been reset."
}

function reset() {
	last_valid_index=$((${#MACHINES[@]} - 1)) # The 0-indexed number of nodes
	for i in $(seq 0 ${last_valid_index}); do
		ssh ${USER}@${MACHINES[$i]}.${DOMAIN} "sudo pkill $1" &
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
elif [[ "$cmd" == "build-run" && "$count" -eq 3 ]]; then
	if [[ "$2" != "debug" && "$2" != "release" ]]; then
		usage
		exit 1
	fi
	if [[ "$2" == "debug" ]]; then
		make DEBUG=1
	else 
		make
	fi
	cl_run "$3"
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