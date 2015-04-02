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

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <spawn.h>
#include <string.h>
#include <fcntl.h>
#include <pwd.h>

#define MAX_PROCESSES 128
#define MAX_ARGS 8192

// Because omg pipe magic is unreadable
#define FD_STDIN 0
#define FD_STDOUT 1
#define READ_END(x) x[0]
#define WRITE_END(x) x[1]

typedef struct ProcInfo_T {
    char* spawn_args[1024];
    char* name;
    int num_args;
    int finished;
    pid_t pid;
    int status;
    int err_fd;
    int err_read_fd;
    int out_fd;
    int out_read_fd;
    int no_stdout;
    int no_stderr;
} ProcInfo;

int num_processes = 0;
ProcInfo processes[MAX_PROCESSES];

void 
sigdeath(int signum) {
    for (int i=0; i<num_processes; i++) {
        kill( processes[i].pid, SIGKILL);
        waitpid(processes[i].pid, &processes[i].status, 0);
        close(processes[i].err_fd);
    }
    exit(1);
}

int main(int argc, char* argv[]) {

    if (argc <= 1) {
        exit(1);
    }

    pid_t pid;
    struct sigaction act;

    int in = 0;

    posix_spawn_file_actions_t action;

    char *path = (char*)malloc(1024);
    int cur_arg = 1;
    int i = 0;
    int rd = 0;

    act.sa_handler = &sigdeath;
    sigemptyset (&act.sa_mask);
    act.sa_flags = 0;

    sigaction (SIGTERM, &act, NULL);
    sigaction (SIGKILL, &act, NULL);
    sigaction (SIGINT,  &act, NULL);
    sigaction (SIGQUIT, &act, NULL);

    if ( strcmp(argv[cur_arg], "--CMR_PIPE_UID" ) == 0 ) {
        seteuid(atoi(argv[cur_arg+1]));
        cur_arg+=2;
    }
    if ( strcmp(argv[cur_arg], "--CMR_PIPE_GID" ) == 0 ) {
        setegid(atoi(argv[cur_arg+1]));
        cur_arg+=2;
    }

    while(cur_arg < argc) { 

        int start_arg = cur_arg;

        int out[2];
        int err[2];

        pipe(out);
        pipe(err);

        // Set read end of err to non-blocking
        fcntl(err[0], F_SETFL, fcntl(err[0], F_GETFL) | O_NONBLOCK);

        processes[i].err_fd = WRITE_END(err);
        processes[i].err_read_fd = READ_END(err); // We'll use this later to collect error output
        processes[i].out_fd = WRITE_END(out);
        processes[i].out_read_fd = READ_END(out);
        processes[i].no_stdout = 0;
        processes[i].no_stderr = 0;;

        while( cur_arg < argc && ! ( argv[cur_arg][0] == ':' ) ) {

            if ( strcmp(argv[cur_arg], "--CMR_NAME" ) == 0 ) {
                cur_arg++;
                processes[i].name = &argv[cur_arg][0];

                cur_arg++;
                continue;
            }

            if ( strcmp(argv[cur_arg], "--CMR_PIPE_OUT" ) == 0 ) {

                cur_arg++;

                sprintf(path, "%s", &argv[cur_arg][0]);
                processes[i].out_fd = open( path, O_WRONLY|O_CREAT, S_IRUSR|S_IXUSR|S_IWUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH );

                if (processes[i].out_fd < 0) {
                    fprintf(stdout, "cmr-pipe failed to open path %s\n", path);
                    exit(1);
                }

                processes[i].no_stdout = 1;
                cur_arg++;

                continue;
            }

            if ( strcmp(argv[cur_arg], "--CMR_PIPE_ERR" ) == 0 ) {
                cur_arg++;

                sprintf(path, "%s", &argv[cur_arg][0]);
                processes[i].err_fd =  open( path, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH );

                if (processes[i].err_fd < 0) {
                    fprintf(stdout, "cmr-pipe failed to open path %s\n", path);
                    exit(1);
                }

                processes[i].no_stderr = 1;
                cur_arg++;

                continue;
            }

            processes[i].spawn_args[processes[i].num_args] = argv[cur_arg];
            processes[i].num_args++;
            cur_arg++;
        }

        if ( ! processes[i].name ) {
            processes[i].name = processes[i].spawn_args[0];
        }

        processes[i].spawn_args[cur_arg - start_arg] = '\0';

        posix_spawn_file_actions_init(&action);
        posix_spawn_file_actions_adddup2(&action, in, 0);

        posix_spawn_file_actions_adddup2(&action, processes[i].out_fd, 1);
        if ( processes[i].out_fd != WRITE_END(out) ) {
            posix_spawn_file_actions_addclose(&action, WRITE_END(out));
            in = 0;
        } else {
            in = READ_END(out);
        }
        posix_spawn_file_actions_addclose(&action, READ_END(out));

        posix_spawn_file_actions_adddup2(&action, processes[i].err_fd, 2);
        if ( processes[i].err_fd != WRITE_END(err) ) {
            posix_spawn_file_actions_addclose(&action, WRITE_END(err));
        }
        posix_spawn_file_actions_addclose(&action, READ_END(err));


        posix_spawnp(&(processes[i].pid), processes[i].spawn_args[0], &action, NULL, processes[i].spawn_args, NULL);
        if ( processes[i].err_fd != WRITE_END(err) ) { // If stderr is redirected, don't attempt to collect it
            close(processes[i].err_fd);
            close(processes[i].err_read_fd);
        }
        close(processes[i].out_fd);
        processes[i].finished = 0;

//        in = READ_END(out);

        num_processes++;
        i++;
        cur_arg++;
    }

    size_t buffer_size = 65535;
    char* buf = (char*)malloc(buffer_size * sizeof(char));


    int running_processes = num_processes;
    int exit_status = 0;
    int rc;

    for ( int j = 0; j < num_processes; j++ ) {
        if (in != processes[j].out_read_fd) {
            close(processes[j].out_read_fd);
        }
    }

    while ( running_processes > 0 ) {
      // Do some waitpids
      for ( int j = 0; j < num_processes; j++ ) {
        if ( exit_status == 1 && processes[j].finished == 0) {
          kill( processes[j].pid, SIGKILL);
        }

        if (in == processes[j].out_read_fd) {
          if ( read(in, buf, buffer_size) <= 0) {
            close(in);
          }
        }

        while ( ( rd = read(processes[j].err_read_fd, buf, buffer_size) ) > 0 ) {
          fprintf(stdout, "%s: %.*s\n", processes[j].name, rd, buf);
        }

        if ( !processes[j].finished ) {
        // Check to see if any of the processes have ended
        rc = waitpid(processes[j].pid, &processes[j].status, WNOHANG);

        if (rc != 0) {
          while ( ( rd = read(processes[j].err_read_fd, buf, buffer_size) ) > 0 ) {
            fprintf(stdout, "%s: %.*s\n", processes[j].name, rd, buf);
          }
          close(processes[j].err_read_fd);

          running_processes--;
          close(processes[j].err_fd);
          processes[j].finished = 1;

          if ( WEXITSTATUS(processes[j].status) != 0 ) {
            // A couple of strange exit codes that we'd rather not produce a failure on (we'll still get warnings in the logs)
            if ( WEXITSTATUS(processes[j].status) == 1 ) {
              if        ( strcmp(processes[j].spawn_args[0], "cat")  == 0 ) {
                continue;
              } else if ( strcmp(processes[j].spawn_args[0], "zcat") == 0 ) {
                continue;
              } else if ( strcmp(processes[j].spawn_args[0], "grep") == 0) {
                continue;
              } else if ( (strcmp(processes[j].spawn_args[0], "sh") == 0) ) {
                if        ( strncmp(processes[j].spawn_args[2], "cat",  sizeof("cat")-1)  == 0 ) {
                  continue;
                } else if ( strncmp(processes[j].spawn_args[2], "zcat", sizeof("zcat")-1) == 0 ) {
                  continue;
                } else if ( strncmp(processes[j].spawn_args[2], "grep", sizeof("grep")-1) == 0 ) {
                  continue;
                }
              }
            }
            exit_status = 1;
          }
        }
        }
      }
      usleep(10000);
    }

    // consume all process' stderr
    for ( int j = 0; j < i; j++ ) {
        while ( ( rd = read(processes[j].err_read_fd, buf, buffer_size) ) > 0 ) {
            fprintf(stdout, "%s: %.*s\n", processes[j].name, rd, buf);
        }
        close(processes[j].err_read_fd);
    }

    exit(exit_status);
}
