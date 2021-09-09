#include <stdio.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <stdarg.h>
#include <pthread.h>

#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/time.h>

typedef struct {
    char *pPath;
    char *pAddr;
    int nPort;
    int nID;
} ClientArgs;

typedef struct {
    char *pData;
    int nSize;
    int nUsed;
} String;

// This string functions are exactly the same in server
// But here we are using them for receiving responses
void stringInit(String *pStr, size_t nSize)
{
    pStr->nUsed = 0;
    pStr->nSize = nSize + 1;
    pStr->pData = malloc(pStr->nSize);

    if (pStr->pData == NULL)
    {
        fprintf(stderr, "Can not alloc memory for string\n");
        exit(EXIT_FAILURE);
    }

    memset(pStr->pData, 0, sizeof(pStr->nSize));
}

// This string functions are exactly the same in server
// But here we are using them for receiving responses
void stringClear(String *pStr)
{
    if (pStr->pData != NULL)
    {
        free(pStr->pData);
        pStr->pData = NULL;
    }
}

// This string functions are exactly the same in server
// But here we are using them for receiving responses
size_t stringAppend(String *pStr, char *pData, size_t nSize)
{
    if (pStr->nSize - pStr->nUsed < nSize)
    {
        pStr->nSize = pStr->nSize + nSize + 1;
        pStr->pData = realloc(pStr->pData, pStr->nSize);
        if (pStr->pData == NULL)
        {
            fprintf(stderr, "Can not realloc memory for string\n");
            exit(EXIT_FAILURE);
        }
    }

    memcpy(pStr->pData + pStr->nUsed, pData, nSize);
    pStr->nUsed += nSize;
    pStr->pData[pStr->nUsed] = '\0';
    return pStr->nUsed;
}

// This timeStamp is exactly the same in server
uint32_t timeStamp()
{
    struct timeval tv; // Get timestamp in usecs
    if (gettimeofday(&tv, NULL) < 0) return 0;
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

// This fucntion creates client socket and connects to newely created socket
int createClientSocket(const char *pAddr, uint16_t nPort)
{
    struct in_addr addr;
    inet_pton(AF_INET, pAddr, &addr);

    struct sockaddr_in sockAddr;
    sockAddr.sin_family = AF_INET;
    sockAddr.sin_port = htons(nPort);
    sockAddr.sin_addr.s_addr = addr.s_addr;;

    int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd < 0)
    {
        fprintf(stderr, "Can not create client socket: %u (%s)\n", nPort, strerror(errno));
        return -1;
    }

    if (connect(fd, (struct sockaddr*)&sockAddr, sizeof(sockAddr)) < 0)
    {
        fprintf(stderr, "Can not connect to the socket: %u (%s)\n", nPort, strerror(errno));
        close(fd);
        return -1;
    }

    return fd;
}

// This function parses command line arguments
void parseArgs(int argc, char *argv[], ClientArgs *pConf)
{
    int nOpt = 0, nCount = 0;

    while ((nOpt = getopt(argc, argv, "a:p:o:i:")) != -1) 
    {
        switch (nOpt)
        {
            case 'p':
                pConf->nPort = atoi(optarg);
                nCount++;
                break;
            case 'a':
                pConf->pAddr = optarg;
                nCount++;
                break;
            case 'i':
                pConf->nID = atoi(optarg);
                nCount++;
                break;
            case 'o':
                pConf->pPath = optarg;
                nCount++;
                break;
            default:
                break;
        }
    }

    // Validate command line arguments
    if (nCount != 4)
    {
        printf("Invalid or missing command line parameters\n");
        printf("Usage: %s -a serverAddr -p PORT -o pathToQueryFile –i clientId\n", argv[0]);
        exit(EXIT_FAILURE);
    }
}

// This function line by line reads sql queries from input file and sends to server, 
// receives responses and prints them in the terminal
int sendQueries(ClientArgs *pArgs)
{
    FILE *fp = fopen(pArgs->pPath, "r");
    if (fp == NULL)
    {
        printf("Can not open intput file: %s", pArgs->pPath);
        return 0;
    }

    char *pLine = NULL;
    size_t nLength = 0;
    ssize_t nRead = 0;
    int nCount = 0;

    // Line-by-line read input csv file
    while ((nRead = getline(&pLine, &nLength, fp)) != -1) 
    {
        int nID = atoi(pLine);
        if (nID != pArgs->nID) continue;

        char *pQuery = strstr(pLine, " ");
        if (pQuery != NULL)
        {
            pQuery += 1;
            char *savePtr;

            char *pParsedQuery = strtok_r(pQuery, "\n", &savePtr);
            if (pParsedQuery != NULL)
            {
                printf("Client-%d connecting to %s:%d\n", pArgs->nID, pArgs->pAddr, pArgs->nPort);
                uint32_t nStartTime = timeStamp();

                // Connect to server
                int nFD = createClientSocket(pArgs->pAddr, pArgs->nPort);
                if (nFD < 0)
                {
                    fprintf(stderr, "Can not connect to server: %s\n", strerror(errno));
                    exit(EXIT_FAILURE);
                }

                printf("Client-%d connected and sending query ‘%s’\n", pArgs->nID, pParsedQuery);

                // Send query to server
                if (write(nFD, pParsedQuery, strlen(pParsedQuery)) < 0){
                    fprintf(stderr, "Can not send query to server: %s\n",strerror(errno));
                    break;
                } 
                
                char sBuffer[512];
                int nBytes = 0;

                // Read response from server and print
                int nCount = 0;
                if (read(nFD, &nCount, sizeof(int)) < 0){
                    fprintf(stderr, "Can not read response from server: %s\n",strerror(errno));
                    break;
                } 

                String response;
                stringInit(&response, 512);

                while ((nBytes = read(nFD, sBuffer, sizeof(sBuffer))) > 0)
                {
                    sBuffer[nBytes] = 0;
                    stringAppend(&response, sBuffer, strlen(sBuffer));
                }

                // Log statistics into file
                uint32_t nEndTime = timeStamp();
                double fDiff = (double)(nEndTime - nStartTime) / (double)1000000;
                printf("Server’s response to Client-%d is %d records, and arrived in %f seconds\n", pArgs->nID, nCount, fDiff);

                int i;
                for (i = 0; i < response.nUsed; i++)
                {
                    if (response.pData[i] == ',')
                        response.pData[i] = '\t';
                }

                printf("%s\n", response.pData);
                stringClear(&response);

                close(nFD);
                nCount++;
            }
        }
    }

    // Clean line and close file
    free(pLine); // this variable is allocated by getline() function
    fclose(fp);

    return nCount;
}


// Main function
int main(int argc, char *argv[])
{
    // Parse arguments
    ClientArgs args;
    parseArgs(argc, argv, &args);

    // Send queries
    int nCount = sendQueries(&args);
    printf("A total of %d queries were executed, client is terminating.\n", nCount);

    return 0;
}