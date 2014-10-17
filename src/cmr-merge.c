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
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <glob.h>
#include <getopt.h>

static struct option long_options[] = {
    {.name = "delimiter",     .has_arg = required_argument, .flag = 0, .val = 'x'},
    {0,0,0,0},
};
static char short_options[] = "x:";

void usage() {
    fprintf(stderr, "Usage: cmr-mergebucket [-x <delimiter] <glob> [<glob ...]\n");
}

#define BUFFER_SIZE 1024*64

typedef struct merge_state_t {
    FILE* file;
    int rd;
    char* buf;
    int skey_len;
} merge_state;

char* next_key;
int   next_skey_len;

int null_stat (const char *path, struct stat *buf) {
    // For everyone's sake...
    return 0;
}


int main( int argc, char* const argv[] ) {
    int option_index = 0;
    glob_t file_glob;
    char delimiter = '\002';

    file_glob.gl_stat = null_stat;
    file_glob.gl_lstat = null_stat;
    file_glob.gl_opendir = (void* (*)(const char*))opendir;
    file_glob.gl_readdir = (struct dirent* (*)(void*))readdir;
    file_glob.gl_closedir = (void (*)(void*))closedir;

    char *pattern;
    int num_files = 0;
    int open_files = 0;
    size_t buffer_size = BUFFER_SIZE;
    char* pos;
    int argi = 1;

    while (1) {
        int opt = getopt_long(argc, argv, short_options, long_options, &option_index);
        if (opt < 0) { break; }
        switch (opt) {
            case 'x': // buffer-size
                argi += 2;
                delimiter = optarg[0];
                buffer_size = atoi(optarg);
                break;
        }
    }

    if (argc <= argi) {
        usage();
        exit(1);
    }

    glob(argv[argi++], GLOB_ALTDIRFUNC|GLOB_BRACE, NULL, &file_glob);

    for ( int i=argi; i<argc; i++ ) {
        glob(argv[i], GLOB_ALTDIRFUNC|GLOB_BRACE|GLOB_APPEND, NULL, &file_glob);
    }

    num_files = file_glob.gl_pathc;

    merge_state* mergy = (merge_state*)calloc(1, num_files*sizeof(merge_state));
    merge_state* next = &mergy[0];

    // The great opening
    for ( int i=0; i<num_files; i++ ) {
        mergy[open_files].file = fopen( file_glob.gl_pathv[i], "rb" );
        if ( mergy[open_files].file ) {
            mergy[open_files].buf = (char*)malloc(buffer_size*sizeof(char));
            open_files++;
        }
    }

    if ( open_files == 0 ) {
        exit(0);
    }

    next_key = (char*)malloc(buffer_size*sizeof(char));
    
    // Fill buffers
    for ( int i=0; i<num_files; i++ ) {
        if ( !mergy[i].file ) { continue; }
        if ( ( mergy[i].rd = getline(&mergy[i].buf, &buffer_size, mergy[i].file) ) <= 0 ) {
            fclose(mergy[i].file);
            mergy[i].file = NULL;
            open_files--;
            continue;
        }
        // Figure out how long the key is
        pos = mergy[i].buf;
        while (pos[0] != delimiter) { pos++; }
        mergy[i].skey_len = pos - mergy[i].buf;
    }
    

    int out = fileno(stdout);

    while( open_files > 0 ) {
        // Find a key...
        for ( int i=0; i<num_files; i++ ) {
            if (mergy[i].file) {
                next = &mergy[i];
                break;
            }
        }

        // Find the next key...
        for ( int i=0; i<num_files; i++ ) {
            if ( !mergy[i].file ) { continue; }
            int cmp_len = next->skey_len < mergy[i].skey_len ? next->skey_len : mergy[i].skey_len;
            int result = memcmp(next->buf, mergy[i].buf, cmp_len);
            //printf("%.*s <=> %.*s  ==  %d\n", next->skey_len, next->buf, mergy[i].skey_len, mergy[i].buf, result);
            if ( result > 0 ) {
                next = &mergy[i];
            }
        }

        memcpy(next_key, next->buf, next->skey_len);
        next_skey_len = next->skey_len;

        // Output this key until there is... no more!
        for ( int i=0; i<num_files; i++ ) {
            if ( !mergy[i].file ) { continue; }
            //printf("%s ... %.*s\n", next_key, next_skey_len, mergy[i].buf);
            while ( next_skey_len == mergy[i].skey_len &&
                    memcmp(next_key, mergy[i].buf, next_skey_len) == 0 ) {
                write( out, mergy[i].buf, mergy[i].rd );
                mergy[i].rd = getline(&mergy[i].buf, &buffer_size, mergy[i].file);
                if ( mergy[i].rd <= 0 ) {
                    // Failed to refill buffer (probably EOF)
                    fclose(mergy[i].file);
                    mergy[i].file = NULL; 
                    open_files--;
                    break;
                }
                // Figure out how long the key is
                pos = mergy[i].buf;
                while (pos[0] != delimiter) { pos++; }
                mergy[i].skey_len = pos - mergy[i].buf;
            }
        }
    }
}
