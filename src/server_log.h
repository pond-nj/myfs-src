#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

FILE * server_log_open(int n)
{
    FILE *logfile;
    
    // very first thing, open up the logfile and mark that we got in
    // here.  If we can't open the logfile, we're dead.
    char log_filename[] = "/home/csci5550/CSCI5550-project/server_log/temp.log";

    printf("Server %d: try create log file %s\n", n, log_filename);

    logfile = fopen(log_filename, "w");

    if (logfile == NULL) {
        perror("logfile");
        exit(EXIT_FAILURE);
    }

    printf("Server %d: created sucessfully\n", n);
    
    // set logfile to line buffering
    setvbuf(logfile, NULL, _IOLBF, 0);

    return logfile;
}

void server_log_msg(FILE* f, const char *format, ...)
{
    va_list ap;
    va_start(ap, format);

    vfprintf(f, format, ap);
}