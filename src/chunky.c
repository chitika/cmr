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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <fcntl.h>
#include <glob.h>
#include <dirent.h>

static struct option long_options[] = {
    {.name = "size",     .has_arg = required_argument, .flag = 0, .val = 's'},
    {0,0,0,0},
};
static char short_options[] = "s:";

int null_stat (const char *path, struct stat *buf) {
    return 0;
}

int main(int argc, char* const argv[]) {
    int argi=1;
    int option_index = 0;
    int buffer_size = 0;
    int min_avail = 1024*1024;
    int fd = fileno(stdin);

    while (1) {
        int opt = getopt_long(argc, argv, short_options, long_options, &option_index);
        if (opt < 0) { break; }
        switch (opt) {
            case 's': // buffer-size
                argi+=2;
                buffer_size = atoi(optarg);
                break;
        }
    }

    if (buffer_size <= 0) { fprintf(stderr, "Usage: buffer -s <size in Mb>\n"); return -1; }
    buffer_size = buffer_size * 1024 * 1024;

    char *buffer = (char*)malloc(buffer_size);

    if ( argi >= argc ) { // STDIN
        while (1) {
            int total_rd = 0;
            int rd = 0;
            do {
                rd = read(fd, &buffer[total_rd], buffer_size-total_rd);
                total_rd += rd;
            } while( rd > 0 && (buffer_size - total_rd) > 0 );
            if ( total_rd == 0 ) { break; } // Didn't read anything, were done
            write(fileno(stdout), buffer, total_rd);
        }
        close(fd);
        close(fileno(stdout));
        free(buffer);
        exit(0);
    }


    // NOT STDIN
    glob_t my_glob;
    int glob_flags = GLOB_ALTDIRFUNC|GLOB_BRACE;
    my_glob.gl_stat = null_stat;
    my_glob.gl_lstat = null_stat;
    my_glob.gl_opendir = (void* (*)(const char*))opendir;
    my_glob.gl_readdir = (struct dirent* (*)(void*))readdir;
    my_glob.gl_closedir = (void (*)(void*))closedir;
    int idx_glob = 0;
    int idx_glob_end = -1;
    int offset = 0;
    int avail = buffer_size;

    while ( argi < argc ) {
        glob(argv[argi], glob_flags, NULL, &my_glob);
        glob_flags = GLOB_ALTDIRFUNC|GLOB_BRACE|GLOB_APPEND;
        idx_glob_end = my_glob.gl_pathc;
        argi++;

        while ( idx_glob < idx_glob_end ) {
            fd = open( my_glob.gl_pathv[idx_glob], O_RDONLY );
            idx_glob++;
            if ( fd < 0 ) { continue; }

            while (1) {
                int total_rd = 0;
                int rd = 0;
                do {
                    rd = read(fd, &buffer[total_rd+offset], avail);
                    total_rd += rd;
                    avail -= rd;
                } while( rd > 0 && avail > 0 );
                if ( total_rd <= 0 ) { break; } // Didn't read anything, were done
/*
                write(fileno(stdout), buffer, total_rd);
                avail = buffer_size;
*/
                if ( avail < min_avail ) {
                    write(fileno(stdout), buffer, total_rd+offset);
                    avail = buffer_size;
                    offset = 0;
                } else {
                    offset += total_rd;
                }

            }
            close(fd);
        }
    }

    if ( offset > 0 ) {
        write(fileno(stdout), buffer, offset);
    }

    close(fileno(stdout));
    free(buffer);
}
