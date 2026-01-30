/**
 * text_server.cc
 *
 * Text_server is half of a client/server pair that shows how to receive text
 * from a client and send a reply.
 */

#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>
#include <unistd.h>
#include <libgen.h>

#include "p2p_node.h"

/** Print a message to inform the user of how to use this program */
void usage(char *progname) {
    printf("%s: Server half of a client/server echo program to demonstrate "
        "sending text over a network.\n",
        basename(progname));
    printf("  -p [int]    Port number of the server\n");
    printf("  -h          Print help (this message)\n");
}

/**
 * In this program, the only useful arguments are a port number and a hostname.
 * We store them in the arg_t struct, which we populate via the get_args()
 * function.
 */

struct arg_t {
    /** The port on which the program will listen for connections */
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
    while ((opt = getopt(argc, argv, "p:h")) != -1) {
        switch (opt) {
        case 'p':
        args.port = atoi(optarg);
        break;
        case 'h':
        args.usage = true;
        break;
        }
    }
}

int main(int argc, char** argv) {
    arg_t args;
    parse_args(argc, argv, args);

    std::cout << "Starting server on port " << args.port << "...\n";
    P2PNode node(args.port);

    // 2. Keep the main thread alive
    // Since P2PNode runs the listener in a background thread, the main thread
    // needs to do something (or just sleep) to prevent the program from exiting immediately.
    std::cout << "Press Enter to stop the server.\n";
    std::cin.get(); // Blocks until user hits Enter

    return 0;
}