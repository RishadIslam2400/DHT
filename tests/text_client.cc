/**
 * text_client.cc
 *
 * Text_client is half of a client/server pair that shows how to send text to a
 * server and get a reply.
 */

#include <iostream>
#include <string>
#include <libgen.h>


#include "p2p_communication_node.h"

/**
 * Display a help message to explain how the command-line parameters for this
 * program work
 *
 * @progname The name of the program
 */
void usage(char *progname) {
    printf("%s: Client half of a client/server echo program to demonstrate "
            "sending text over a network.\n",
            basename(progname));
    printf("  -s [string] Name of the server (probably 'localhost')\n");
    printf("  -p [int]    Port number of the server\n");
    printf("  -h          Print help (this message)\n");
}

/** arg_t is used to store the command-line arguments of the program */
struct arg_t {
    /** The name of the server to which the parent program will connect */
    std::string server_name = "";

    /** The port on which the program will connect to the above server */
    size_t port = 0;

    /** Is the user requesting a usage message? */
    bool usage = false;
};

/**
 * Parse the command-line arguments, and use them to populate the provided args
 * object.
 *
 * @param argc The number of command-line arguments passed to the program
 * @param argv The list of command-line arguments
 * @param args The struct into which the parsed args should go
 */
void parse_args(int argc, char **argv, arg_t &args) {
    long opt;
    while ((opt = getopt(argc, argv, "p:s:h")) != -1) {
        switch (opt) {
        case 's':
        args.server_name = std::string(optarg);
        break;
        case 'p':
        args.port = atoi(optarg);
        break;
        case 'h':
        args.usage = true;
        break;
        }
    }
}

int main(int argc, char **argv) {
    arg_t args;
    parse_args(argc, argv, args);

    if (args.usage) {
        usage(argv[0]);
        return 0;
    }

    // 1. Create the node
    // Clients in P2P also need a port to listen on, even if they initiate connections.
    // We can pick a random port (0) or a specific client port (e.g., 9000).
    // For this simple example, we might not strictly need to listen if we only send,
    // but P2PNode requires a port. Let's pass 0 (OS picks ephemeral port) or a fixed one.
    P2PNode node(0); 

    // Connect
    std::cout << "Connecting to " << args.server_name << ":" << args.port << "...\n";
    int socket_fd = node.connect_to_node(args.server_name, args.port);

    if (socket_fd < 0) {
        std::cerr << "Could not connect to server.\n";
        return 1;
    }

    // Start the Chat/Echo loop
    node.echo_client(socket_fd, true);

    // Cleanup
    close(socket_fd);
    return 0;
}