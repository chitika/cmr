/*  Copyright (C) 2014 Chitika Inc.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <fcntl.h>
#include <spawn.h>

#include "MurmurHash3.h"

static struct option long_options[] = {
    {.name = "destination",    .has_arg = required_argument, .flag = 0, .val = 'd'},
    {.name = "delimiter",      .has_arg = required_argument, .flag = 0, .val = 'x'},
    {.name = "prefix",         .has_arg = required_argument, .flag = 0, .val = 'p'},
    {.name = "num-partitions", .has_arg = required_argument, .flag = 0, .val = 'n'},
    {.name = "map-id",         .has_arg = required_argument, .flag = 0, .val = 'm'},
    {.name = "sort",           .has_arg = no_argument,       .flag = 0, .val = 'S'},
    {.name = "strip-joinkey",  .has_arg = no_argument,       .flag = 0, .val = 's'},
    {.name = "join",           .has_arg = no_argument,       .flag = 0, .val = 'j'},
    {0,0,0,0},
};
static char short_options[] = "d:D:x:p:k:a:n:m:Ssj";

void usage() {
    fprintf(stderr, "Usage: <input-stream> | cmr-bucket -x <delimiter> -d <destination folder> -n <num-partitions> -m <map-id> [-p <prefix> -s <sort>]\n");
}

// Because omg pipe magic is unreadable
#define FD_STDIN 0
#define FD_STDOUT 1
#define READ_END(x) x[0]
#define WRITE_END(x) x[1]

int main( int argc, char* const argv[] ) {
    const char* prefix = "part";
    const char* destination = ".";
    char delimiter = '\001';

    int option_index = 0;
    int num_partitions = -1;
    int map_id = -1;
    int join = 0;
    int strip_joinkey = 0;
    int sort = 0;

    while (1) {
        int opt = getopt_long(argc, argv, short_options, long_options, &option_index);
        if (opt < 0) { break; }
        switch (opt) {
            case 'd': // destination
                destination = optarg;
                break;
            case 'x': // delimiter
                delimiter = optarg[0];
                break;
            case 'p': // prefix
                prefix = optarg;
                break;
            case 'n': // num-partitions
                num_partitions = atoi(optarg);
                break;
            case 'm': // map-id
                map_id = atoi(optarg);
                break;
            case 'S': // sort
                sort = 1;
                break;
            case 's': // strip-joinkey
                strip_joinkey = 1;
                break;
            case 'j': // join
                join = 1;
                break;
            default:
                usage();
                exit(1);
                break;
        }
    }

    if ( num_partitions == -1 || map_id == -1 ) {
        usage();
        exit(1);
    }

    char *chunky_bin_path = (char*)malloc(256);
    sprintf(chunky_bin_path, "chunky");
    char *chunky_size_flag = (char*)malloc(256);
    sprintf(chunky_size_flag, "-s");
    char *chunky_size = (char*)malloc(256);
    sprintf(chunky_size, "4");

    char * chunkyArgs[] = { chunky_bin_path, chunky_size_flag, chunky_size, NULL };


    char *sort_bin_path = (char*)malloc(256);
    sprintf(sort_bin_path, "/usr/bin/sort");
    char *sort_arg = (char*)malloc(256);
    sprintf(sort_arg, "--buffer-size=16M");


    pid_t pids[1024];
    posix_spawn_file_actions_t action;
    char * spawnedArgs[] = { sort_bin_path, sort_arg, NULL };

    unsigned __int128 kr_max = -1;
    unsigned __int128 kr_size = kr_max / num_partitions;

    int* write_fds = (int*)calloc(num_partitions, sizeof(int));
    
    int failed = 0;
    int nagg = 0;
    int nkey = 0;
    int rd = 0;
    size_t buffer_size = 65535*4;
    char* buf = (char*)malloc(buffer_size * sizeof(char));
    char* path = (char*)malloc(4096);
    char* pos = buf;
    char* joinkey_pos = buf;
    __int128 key;

    int so_many_processes = 0;

    while ( ( rd = getline(&buf, &buffer_size, stdin) ) > 0 ) {
        failed = 0;
        nagg = 0;
        nkey = 0;
        joinkey_pos = buf;

        if (strip_joinkey) {
            while(joinkey_pos[0] != delimiter) {
                joinkey_pos++;
                if (joinkey_pos > buf+rd) { failed = 1; break; }
            }
            joinkey_pos++;
        }
        if (failed) { continue; }

        if (join) {
            pos = buf;
            while(pos[0] != delimiter) {
                pos++;
                if (pos > buf+rd) { failed = 1; break; }
            }
        } else {
            pos = joinkey_pos;
            while(pos[0] != delimiter) {
                pos++;
                if (pos > buf+rd) { failed = 1; break; }
            }
        }
        if (failed) { continue; }

        if (strip_joinkey) {
            // If we're stripping the join key, don't use it as part of the hash
            MurmurHash3_x64_128( joinkey_pos, pos-joinkey_pos, 0, &key );
        }
        else {
            MurmurHash3_x64_128( buf, pos-buf, 0, &key );
        }

        int out_id = (int) (key / kr_size);

        if ( write_fds[out_id] == 0 ) {
            // oh god, forks
            int sort_pipe[2];
            int chunky_pipe[2];

            if (sort) {
                pipe(sort_pipe);
            }

            pipe(chunky_pipe);


            // -- Open up the output file
            sprintf(path, "%s/%s-%d-%d", destination, prefix, map_id, out_id);
            int fd = open( path, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH );

            if (sort) {
                // -- Spawn Sort

                posix_spawn_file_actions_init(&action);
                posix_spawn_file_actions_adddup2(&action,  READ_END(sort_pipe),    FD_STDIN);
                posix_spawn_file_actions_adddup2(&action,  WRITE_END(chunky_pipe), FD_STDOUT);

                posix_spawn_file_actions_addclose(&action, READ_END(chunky_pipe));
                posix_spawn_file_actions_addclose(&action, WRITE_END(sort_pipe));
                posix_spawn_file_actions_addclose(&action, fd);

                posix_spawnp(&pids[so_many_processes++], spawnedArgs[0], &action, NULL, spawnedArgs, NULL);
            }


            // -- Spawn Chunky
            posix_spawn_file_actions_init(&action);

            posix_spawn_file_actions_adddup2(&action, READ_END(chunky_pipe), FD_STDIN);
            posix_spawn_file_actions_adddup2(&action, fd,                    FD_STDOUT);

            posix_spawn_file_actions_addclose(&action, WRITE_END(chunky_pipe));

            if (sort) {
                posix_spawn_file_actions_addclose(&action, READ_END(sort_pipe));
                posix_spawn_file_actions_addclose(&action, WRITE_END(sort_pipe));
            }

            posix_spawnp(&pids[so_many_processes++], chunkyArgs[0], &action, NULL, chunkyArgs, NULL);

            // Done spawning processes, clean up fds on this process

            // this process doesn't need to write to the output file
            close(fd);

            // this process doesn't read from sort or chunky
            if (sort) {
                close(READ_END(sort_pipe));
            }
            close(READ_END(chunky_pipe));


            if (sort) {
              // if sorting, this process isn't writing to chunky
              close(WRITE_END(chunky_pipe));
              write_fds[out_id] = WRITE_END(sort_pipe);
            } else {
              write_fds[out_id] = WRITE_END(chunky_pipe);
            }
            
            // Set close on exec on our write fd, we don't want this fd in any subsequent spawned process
            fcntl(write_fds[out_id], F_SETFD, FD_CLOEXEC);
        }

        // Done pipe magic

        // Write the data
        if (strip_joinkey) {
            write( write_fds[out_id], joinkey_pos, rd - (joinkey_pos - buf) );
        }
        else {
            write( write_fds[out_id], buf, rd );
        }
    }

    // Done processing, close all of our write fds
    for ( int i=0; i<num_partitions; i++ ) {
        if ( write_fds[i] != 0 ) { close(write_fds[i]); }
    }

    // Wait for all of our children to die...
    int status;
    for ( int i=0; i<so_many_processes; i++ ) {
        waitpid(pids[i], &status, 0);
    }

}
