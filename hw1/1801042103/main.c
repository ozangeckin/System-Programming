/*
CSE344-SYSTEM PROGRAMMING-HW1
    Ozan GEÇKİN
    1801042103
*/
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <stdio.h>
#include <unistd.h>
#include <stddef.h>
#include <libgen.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <signal.h>
#include <ctype.h>


// A structure to keep all the parameters in one variable
struct Parameters {
    char *path,
         *permissions,
         *regex,
         type;    
         
    long fileSize,
         numLinks;
};

int RepeatCount(char *src, int start, int len, char c);
bool RegexFilename(char *regex, char *fileName);
void CheckDirectory (char* dirname, struct Parameters* params);
void PrintUsage();
bool ValidateEntry (char* fullPath, char* dirname, struct Parameters* params);
void PrintPath (char* path);
void PrintSplitPath (char* path, char* lastPtr, int depth);
bool CheckType (mode_t mode, char type);
bool CheckPermissions (char* perms, mode_t mode);
void SignalHandler (int signal);

char* lastPath = NULL;
bool ctrl_c = false;

int main(int argc, char **argv)
{
    // Setting up the signal handler to answer to ctrl+c
    struct sigaction act;
    act.sa_handler = SignalHandler;
    sigemptyset (&act.sa_mask);
    act.sa_flags = 0;
    sigaction(SIGINT, &act, NULL);


    // initialize structure
    struct Parameters params = {
        NULL,
        NULL,
        NULL,
        ' ',
        -1L,
        -1L
    };

    char *end = NULL,
         *permisionTest = "rwx";

    DIR *d;
    
    int c, i, j;

    // prevent getopt from shwoing it's own errors
    opterr = 0;

    // Use getopt to read the options
    while ((c = getopt(argc, argv, "f:b:t:p:l:w:")) != -1)
    {
        switch (c)
        {
        case 'w':
            params.path = optarg;
            break;

        case 'f':
            params.regex = optarg;
            break;

        case 'b': // if not an int then print usage
            params.fileSize = strtol(optarg, &end, 10);
            if (*end != '\0')
            {
                PrintUsage(basename(argv[0]));
            }

            break;

        case 't': // check if type is one of the valid options
            if (strlen (optarg) > 1 || ! strchr ("dsbcfpl", optarg[0] ))
                PrintUsage(basename(argv[0]));

            params.type = optarg[0];
            break;

        case 'p': // permissions is longer that 9 show usage
            params.permissions = optarg;
            if (strlen(params.permissions) != 9)
                PrintUsage(basename(argv[0]));

            break;

        case 'l': // if not an int then print usage
            params.numLinks = strtol(optarg, &end, 10);
            if (*end != '\0')
            {
                PrintUsage(basename(argv[0]));
            }

            break;

        default: // show usage for unknown parameters
            PrintUsage(basename(argv[0]));
        }
    }

    // if w and at least one other option isn't entered show usage
    if (params.path == NULL || !(params.regex != NULL || 
                                 params.fileSize != -1 || 
                                 params.permissions != NULL || 
                                 params.numLinks != -1 ||
                                 params.type != ' '))

        PrintUsage(basename(argv[0]));


    // check if the directory entered exists
    d = opendir (params.path);

    if (!d) 
    {
        fprintf (stderr, "%s not found or unreadable\n", params.path);
        return 1;
    }

    closedir(d);

    // check if the permission has invalid characters 
    // only - or rwx at specific positions are valid
    if (params.permissions)
        for (i = 0; i < 3; i++)
            for (j = 0; j < 3; j++)
                if (params.permissions[i * 3 + j] != '-' && 
                    params.permissions[i * 3 + j] != permisionTest[j]) {

                    fprintf (stderr, "%s contains invalid characters\n", params.permissions);
                    return 1;

                }
    

    // Start processing
    CheckDirectory (params.path, &params);

    if (!lastPath)
        printf ("No file found\n");

    free(lastPath);

    return 0;
}

// Checks if each parameter has the default value and if not
// checks if the path entered fulfill the requirements
bool ValidateEntry (char* fullPath, char* dirname, struct Parameters* params)
{
    struct stat stats;
    memset(&stats, 0, sizeof(struct stat));

    stat (fullPath, &stats);

    if (params->type != ' ' && !CheckType (stats.st_mode, params->type))
        return false;

    if (params->regex != NULL && !RegexFilename(params->regex, dirname))
        return false;

    if (params->fileSize != -1L && params->fileSize != stats.st_size)
        return false;

    if (params->numLinks != -1L && params->numLinks != stats.st_nlink)
        return false;

    if (params->permissions != NULL && !CheckPermissions (params->permissions, stats.st_mode))
        return false;

    return true;
}

bool CheckPermissions (char* perms, mode_t mode)
{
    // Each permission mask is entered at the same location it would be on the string
    int p[] = {S_IRUSR,S_IWUSR,S_IXUSR, S_IRGRP,S_IWGRP,S_IXGRP, S_IROTH,S_IWOTH,S_IXOTH};

    int i;

    // Check if the permission at each location fits against the one on the string
    for (i = 0; i < strlen(perms);i++)
        if ( (perms[i] != '-' && ! (mode & p[i])) ||
             (perms[i] == '-' &&   (mode & p[i])) )
            return false;

    return true;
}

// Validates the current file mode against the type entered on the parameters
bool CheckType (mode_t mode, char type)
{
    switch (type)
    {
        case 'd':
            if (!S_ISDIR(mode))
                return false;

            break;

        case 's':
            if (!S_ISSOCK(mode))
                return false;

            break;

        case 'b':
            if (!S_ISBLK(mode))
                return false;

            break;

        case 'c':
            if (!S_ISCHR(mode))
                return false;

            break;

        case 'f':
            if (!S_ISREG(mode))
                return false;

            break;
        
        case 'p': 
            if (!S_ISFIFO(mode))
                return false;

            break;
        
        case 'l':
            if (!S_ISLNK(mode))
                return false;
            
            break;
        
        default:
            return false;
    }

    return true;
}

// traverse directories recursively and check each file or dir entry against the parameters
void CheckDirectory (char* dirname, struct Parameters* params)
{
    struct dirent *dir;
    DIR* d = opendir (dirname);
    char* tmpPath;

    if (d) {
        while ( (dir = readdir (d)) != NULL)
        {
            // exit if ctrl+c was called
            if (ctrl_c) {
                break;
            }

            if (strcmp(dir->d_name,".")!=0 && strcmp(dir->d_name,"..")!=0) {
                // create a variable to store the full path since dir->d_name only contains the current dir name
                tmpPath = malloc (sizeof(char) * (strlen (dirname) + strlen(dir->d_name) + 2));
                sprintf (tmpPath, "%s/%s", dirname, dir->d_name);
                
                if (ValidateEntry(tmpPath, dir->d_name, params))
                    PrintPath(tmpPath);

                if(dir->d_type == DT_DIR) 
                    CheckDirectory(tmpPath, params);   
                
                free(tmpPath);
            }
        }
        
        closedir(d);
    }
}

void PrintUsage(char *app)
{
    fprintf(stderr, "Usage: %s -w path [-f filename] [-b file_size] [-t file_type] [-p permissions] [-l num_links] \n", app);
    fprintf(stderr, "    -w: the path in which to search recursively (i.e. across all of its subtrees)\n");
    fprintf(stderr, "    -f : filename (case insensitive), supporting the following regular expression: +\n");
    fprintf(stderr, "    -b : file size (in bytes)\n");
    fprintf(stderr, "    -t : file type (d: directory, s: socket, b: block device, c: character device f: regular file, p: pipe, l: symbolic link)\n");
    fprintf(stderr, "    -p : permissions, as 9 characters (e.g. ‘rwxr-xr--’)\n");
    fprintf(stderr, "    -l: number of links\n");
    exit(1);
}

// helper function for the regex. 
// In case that a + is found this function reads all the repetitions
int RepeatCount(char *src, int start, int len, char c)
{
    int count = 0,
        i = start;

    while (tolower(src[i]) == c && i < start + len)
    {
        i++;
        count++;
    }

    return count;
}

bool RegexFilename(char *regex, char *fileName)
{
    int regLen = strlen(regex),
        fileLen = strlen(fileName),
        i = 0,
        regI = 0,
        repeat = 0;

    // traverse both the regex entered and the filename until it reaches the end of one of them
    while (i < fileLen && regI < regLen)
    {
        // if + found call the helper function
        if (regex[regI] == '+' && regI > 0)
        {
            repeat = RepeatCount(fileName, i, fileLen, tolower(regex[regI-1]));
            i += repeat;
        } 
        else if (tolower(regex[regI]) != tolower(fileName[i])) // if one character is different return false
        {
            return false;
        }
        else
        {
            i++; // if no repetitions then move the index one forward
        }

        // always move the regex index forward
        regI++;
    }

    // if both indexes managed to get to then end of their string 
    // then the filename fulfills the regex
    return regI == regLen && i == fileLen;
}

// This method prints the paths. It keeps track of the previously printed one
// and only prints the differences. It has to copy the strings because strtok_r 
// alters the original string and we need them as is afterwards. It tokenize the
// last used path and the current one and soon as it finds a difference it puts 
// the token back by keeping a varible for the previous token and setting the 
// separator to its original value. strtok always replace the separator after 
// the current token for a \0
void PrintPath(char* path)
{
    char *lastPtr = NULL,
         *currPtr = NULL,
         *prevPtr = NULL,
         *origPath = NULL,
         *lastToken,
         *currToken;

    int depth = 1;

    origPath = malloc (sizeof(char) * strlen (path) + 1);
    strcpy (origPath, path);

    if (lastPath == NULL) {
        PrintSplitPath (path, lastPtr, 0);
    } else {
        strtok_r (lastPath, "/", &lastPtr);
        strtok_r (path, "/", &currPtr);
        prevPtr = currPtr;

        while (true) {
            lastToken = strtok_r (NULL, "/", &lastPtr);
            currToken = strtok_r (NULL, "/", &currPtr);

            if (currToken == NULL || lastToken == NULL || strcmp (lastToken, currToken))
                break;

            prevPtr = currPtr;
            depth++;
        }

        *(prevPtr-1) = '/'; 
        if (currPtr)
            *(currPtr-1) = '/'; 

        PrintSplitPath (path, prevPtr, depth);
    }

    free (lastPath);
    lastPath = origPath;
    strcpy (path, origPath);

}


// Print each toke on a different line
void PrintSplitPath (char* path, char* lastPtr, int depth)
{
    char* token;
    int i;

    // if at the root just print the name
    if (depth == 0) {
        token = strtok_r (path, "/", &lastPtr);
        printf ("%s\n", token);
        depth++;
    }

    // keep priting each token and moving them two characters
    // to the side by using depth
    while ( (token = strtok_r (NULL, "/", &lastPtr)) != NULL)
    {
        printf ("|");
        for (i = 0; i < (depth * 2) - 1;i++)
            printf ("-");

        printf("%s\n", token);
        depth++;
    }
}

// sets global var to true in order to know if ctrl
// was called.
void SignalHandler (int signal)
{
    ctrl_c = true;
}
