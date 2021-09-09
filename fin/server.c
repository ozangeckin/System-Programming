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

#ifdef LINE_MAX
#define DATA_MAX    LINE_MAX
#else
#define DATA_MAX    2048
#endif
#define LOG_MAX     (1024 * 8)

// Log types
#define ERROR 0
#define INFO  1

typedef struct {
    const char *pLogFile;
    const char *pDBPath;
    int nPoolSize;
    int nPort;
} ServerConfig;

typedef struct {
    char sData[DATA_MAX];
} RowData;

typedef struct {
    pthread_rwlock_t rwLock;
    char sColumns[DATA_MAX];
    RowData *pRows;
    int nColumnCount;
    int nRowCount;
    int nRowSize;
    int isInit;
} Database;

typedef struct {
    pthread_mutex_t mutex;
    const char *pPath;
    int isInit;
} Logger;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int nInterrupt;
    int nWorkerID;
    int nClientFD;
    int isInit;
    int nBusy;
} WorkerContext;

typedef struct {
    WorkerContext *pWorkers;
    int nWorkerCount;
    int isInit;
} WorkerThreads;

// Global variables for gracefull termination
static int g_nListenerSock = -1;
static int g_nInterrupted = 0;
static int g_syncInit = 0;
static pthread_mutex_t g_mutex;
static pthread_cond_t g_cond;
static WorkerThreads g_workers;
static Database g_dataBase;
static Logger g_logger;

// Forward declarations
void destroyDatabase(Database *pDB);
void exitFailure(const char *pMessage);
void lockMutex(pthread_mutex_t *pMutex);
void unlockMutex(pthread_mutex_t *pMutex);
void destroyWorker(WorkerContext *pCtx);
void logToFile(int nType, char *pStr, ...);

////////////////////////////////////////////////////////////////////////
// DYNAMIC STRINGS
////////////////////////////////////////////////////////////////////////

typedef struct {
    char *pData;
    int nSize;
    int nUsed;
} String;

// This function removes specified character from back, 
// we need this function to remove new line characters (\n)
// from the line while parsing input csv file 
void removeCharacter(char *pDst, int nSize, const char *pStr, char nChar)
{
    // This part is only removing new line character from string, nothing more
    int nLen = snprintf(pDst, nSize, "%s", pStr);
    while (nLen > 0)
    {
        if (pDst[nLen--] == nChar)
        {
            pDst[nLen+1] = '\0';
            if (nChar == '\n') break;
        }
    }
}

// Function initializes dynamically alloated string
// We need this string to assemble responses with various size
void stringInit(String *pStr, size_t nSize)
{
    pStr->nUsed = 0;
    pStr->nSize = nSize + 1;
    pStr->pData = malloc(pStr->nSize);

    if (pStr->pData == NULL)
    {
        logToFile(ERROR, "Can not alloc memory for string");
        exitFailure(NULL);
    }

    memset(pStr->pData, 0, sizeof(pStr->nSize));
}

// This function cleans string and frees its allocated memory
void stringClear(String *pStr)
{
    if (pStr->pData != NULL)
    {
        free(pStr->pData);
        pStr->pData = NULL;
    }
}

// This function reallocates string size and appends new data to the string
size_t stringAppend(String *pStr, char *pData, size_t nSize)
{
    if (pStr->nSize - pStr->nUsed < nSize)
    {
        pStr->nSize = pStr->nSize + nSize + 1;
        pStr->pData = realloc(pStr->pData, pStr->nSize);
        if (pStr->pData == NULL)
        {
            logToFile(ERROR, "Can not realloc memory for string");
            exitFailure(NULL);
        }
    }

    memcpy(pStr->pData + pStr->nUsed, pData, nSize);
    pStr->nUsed += nSize;
    pStr->pData[pStr->nUsed] = '\0';
    return pStr->nUsed;
}

////////////////////////////////////////////////////////////////////////
// SIMPLE UTILS
////////////////////////////////////////////////////////////////////////

// This function returnes timestamps in usecs
uint32_t timeStamp()
{
    struct timeval tv; // Get timestamp in usecs
    if (gettimeofday(&tv, NULL) < 0) return 0;
    return tv.tv_sec * 1000000 + tv.tv_usec;
}

// This function receives various arguments, opens file and writes input into file
void logToFile(int nType, char *pStr, ...)
{
    // Log logger mutex
    lockMutex(&g_logger.mutex);
    uint32_t nTimeStamp = timeStamp();

    // Init va args
    va_list args;
    va_start(args, pStr);

    // Serialize various arguments
    char sInput[LOG_MAX];
    vsnprintf(sInput, sizeof(sInput), pStr, args);
    va_end(args);

    // Open output file
    FILE *pFile = fopen(g_logger.pPath, "a");
    if (pFile == NULL) exitFailure("Can not open log file");

    // Append log to file
    if (nType == INFO) fprintf(pFile, "[%u] %s\n", nTimeStamp, sInput);
    else fprintf(pFile, "[%u] %s: %s\n", nTimeStamp, sInput, strerror(errno));

    // Close file and unlock mutex
    fclose(pFile);
    unlockMutex(&g_logger.mutex);
}

////////////////////////////////////////////////////////////////////////
// EXIT RELATED STUFF
////////////////////////////////////////////////////////////////////////

// This function destroys absolutely all allocated memory by server
void globalDestroy()
{
    // Stop and destroy worker threads
    if (g_workers.isInit)
    {
        int i;
        for (i = 0; i < g_workers.nWorkerCount; i++)
        {
            WorkerContext *pWorker = &g_workers.pWorkers[i];
            destroyWorker(pWorker); // Cleanup worker related data
        }

        g_workers.isInit = 0;
    }

    logToFile(INFO, "All threads have terminated, server shutting down.");
    
    // Close listener socket
    if (g_nListenerSock >= 0)
    {
        close(g_nListenerSock);
        g_nListenerSock = -1;
    }

    // Cleanup database
    destroyDatabase(&g_dataBase);

    // Destroy logger
    pthread_mutex_destroy(&g_logger.mutex);
    g_logger.isInit = 0;

    if (g_syncInit)
    {
        // Destroy global sync if initialized
        pthread_mutex_destroy(&g_mutex);
        pthread_cond_destroy(&g_cond);
        g_syncInit = 0;
    }

    // This is not any kind of synchronization
    // Just making valgrind happy, nothing more
    usleep(10000);
}

// This function prints error mesage, cleanups allocated memory and exits with failure
void exitFailure(const char *pMessage)
{
    if (pMessage != NULL) // Display error message
        fprintf(stderr, "%s: %s\n", pMessage, strerror(errno));

    // Cleanup and exit
    globalDestroy();
    exit(EXIT_FAILURE);
}

// SIGINT signal callback
void exitHandler(int sig)
{
    if (sig == SIGINT)
        logToFile(INFO, "Termination signal received, waiting for ongoing threads to complete.");

    g_nInterrupted = 1;
}

////////////////////////////////////////////////////////////////////////
// CONDITION VARIABLES
////////////////////////////////////////////////////////////////////////

// This function just calls pthread_cond_wait() and exits if call is not successfull, nothing more
void waitCondition(pthread_cond_t *pCond, pthread_mutex_t *pMutex)
{
    if (pthread_cond_wait(pCond, pMutex))
    {
        logToFile(ERROR, "Can not wait condition variable");
        exitFailure(NULL);
    }
}

// This function just calls pthread_cond_signal() and exits if call is not successfull, nothing more
void signalCondition(pthread_cond_t *pCond)
{
    if (pthread_cond_signal(pCond))
    {
        logToFile(ERROR, "Can not signal condition variable");
        exitFailure(NULL);
    }
}

////////////////////////////////////////////////////////////////////////
// MUTEX AND LOCKS
////////////////////////////////////////////////////////////////////////

void initMutex(pthread_mutex_t *pMutex)
{
    pthread_mutexattr_t mutexAttr;
    if (pthread_mutexattr_init(&mutexAttr) ||
        pthread_mutexattr_settype(&mutexAttr, PTHREAD_MUTEX_RECURSIVE) ||
        pthread_mutex_init(pMutex, &mutexAttr) ||
        pthread_mutexattr_destroy(&mutexAttr))
    {
        exitFailure("Can not initialize mutex");
    }
}


// This function just calls pthread_mutex_lock() and exits if call is not successfull, nothing more
void lockMutex(pthread_mutex_t *pMutex)
{
    if (pthread_mutex_lock(pMutex))
        exitFailure("Can not lock mutex");
}


// This function just calls pthread_mutex_unlock() and exits if call is not successfull, nothing more
void unlockMutex(pthread_mutex_t *pMutex)
{
    if (pthread_mutex_unlock(pMutex))
        exitFailure("Can not unlock mutex");
}


// This function just calls pthread_rwlock_wrlock() and exits if call is not successfull, nothing more
void lockWrite(pthread_rwlock_t *pLock)
{
    if (pthread_rwlock_wrlock(pLock))
        exitFailure("Can not lock mutex");
}


// This function just calls pthread_rwlock_rdlock() and exits if call is not successfull, nothing more
void lockRead(pthread_rwlock_t *pLock)
{
    if (pthread_rwlock_rdlock(pLock))
        exitFailure("Failet to read lock");
}


// This function just calls pthread_rwlock_unlock() and exits if call is not successfull, nothing more
void unlockRW(pthread_rwlock_t *pLock)
{
    if (pthread_rwlock_unlock(pLock))
        exitFailure("Failet to unloc rw lock");
}

////////////////////////////////////////////////////////////////////////
// SOCKETS
////////////////////////////////////////////////////////////////////////

// This function creates server socket, binds the port and listens to the newely created socket
int createServerSocket(int nPort)
{
    struct sockaddr_in inaddr;
    inaddr.sin_family = AF_INET;
    inaddr.sin_port = htons(nPort);
    inaddr.sin_addr.s_addr = htonl(INADDR_ANY);;

    // Create listener socket file descriptor
    int fd = socket(AF_INET, SOCK_STREAM | FD_CLOEXEC, IPPROTO_TCP);
    if (fd < 0)
    {
        logToFile(ERROR, "Can not create server socket");
        exitFailure(NULL);
    }

    // Bind socket
    if (bind(fd, (struct sockaddr*)&inaddr, sizeof(inaddr)) < 0)
    {
        logToFile(ERROR, "Failed to bind socket");
        close(fd);
        exitFailure(NULL);
    }

    // Listen to socket
    if (listen(fd, 120000) < 0) 
    {
        logToFile(ERROR, "Failed to listen socket");
        close(fd);
        exitFailure(NULL);
    }

    return fd;
}

////////////////////////////////////////////////////////////////////////
// DATABASE
////////////////////////////////////////////////////////////////////////

// This function initializes database structure
void initDatabase(Database *pDB)
{
    // Init read/write lock
    if (pthread_rwlock_init(&pDB->rwLock, NULL))
    {
        logToFile(ERROR, "Can not init rw lock");
        exitFailure(NULL);
    }

    // Initial values
    pDB->sColumns[0] = '\0';
    pDB->nColumnCount = 0;
    pDB->nRowCount = 0;
    pDB->nRowSize = 10;

    // Allocate memory for rows
    pDB->pRows = (RowData*)malloc((sizeof(RowData) + DATA_MAX) * pDB->nRowSize);
    if (pDB->pRows == NULL)
    {
        // Destroy rw lock and free database
        logToFile(ERROR, "Can not alloc memory for database");
        pthread_rwlock_destroy(&pDB->rwLock);
        exitFailure(NULL);
    }

    pDB->isInit = 1;
}

// This function destroys database structure and all associated variables
void destroyDatabase(Database *pDB)
{
    if (!pDB->isInit) return;

    // Clear columns
    if (pDB->pRows != NULL)
    {
        free(pDB->pRows);
        pDB->pRows = NULL;
    }

    // Destroy read/write lock
    pthread_rwlock_destroy(&pDB->rwLock);
}

// This function appends the row data into the database
void appendDatabase(Database *pDB, const char *pRowData)
{
    // Reallocate rows array if upper bound is reached
    if (pDB->nRowCount + 1 > pDB->nRowSize)
    {
        pDB->nRowSize = pDB->nRowSize * 2;
        pDB->pRows = realloc(pDB->pRows, (sizeof(RowData) + DATA_MAX) * pDB->nRowSize);
        if (pDB->pRows == NULL)
        {
            logToFile(ERROR, "Can not realloc memory for columns");
            exitFailure(NULL);
        }
    }

    char sRow[DATA_MAX]; // Remove new line character
    removeCharacter(sRow, sizeof(sRow), pRowData, '\n');

    // Get new/unused row from column and set data
    RowData *pRow = &pDB->pRows[pDB->nRowCount++];
    snprintf(pRow->sData, sizeof(pRow->sData), "%s", sRow);
}

// This function opens database file, line-by-line reads it and saves recordings in the Database structure
void loadDatabase(const char *pPath, Database *pDB)
{
    logToFile(INFO, "Loading dataset...");
    uint32_t nStartTime = timeStamp();

    // Open input csv file read input csv file
    FILE *fp = fopen(pPath, "r");
    if (fp == NULL)
    {
        logToFile(ERROR, "Can not open dataset (%s)", pPath);
        exitFailure(NULL);
    }

    char *pLine = NULL;
    ssize_t nRead = 0;
    size_t nLength = 0;
    int nColumnsParsed = 0;

    // Line-by-line read input csv file
    while ((nRead = getline(&pLine, &nLength, fp)) != -1) 
    {
        // Check if parsed line is valid
        if (pLine != NULL && nLength > 1)
        {
            // If columns are not initialized, parse them first
            if (!nColumnsParsed)
            {
                // Save line into columns and remove new line character from back
                removeCharacter(pDB->sColumns, sizeof(pDB->sColumns), pLine, '\n');

                // Parse count of columnts by counting ','
                char *pOffset = pDB->sColumns;
                while ((pOffset = strstr(pOffset, ",")) != NULL)
                {
                    pOffset++;
                    pDB->nColumnCount++;
                }

                // Last column after ','
                if (pDB->nColumnCount) pDB->nColumnCount++;

                // Mark columns as initialized
                nColumnsParsed = 1;
                continue;
            }

            // Append row into database
            appendDatabase(pDB, pLine);
        }
    }

    // Log statistics into file
    uint32_t nEndTime = timeStamp();
    double fDiff = (double)(nEndTime - nStartTime) / (double)1000000;
    logToFile(INFO, "Dataset loaded in %f seconds with %d records.", fDiff, pDB->nRowCount);

    // Clean line and close file
    free(pLine); // this variable is allocated by getline() function
    fclose(fp);
}

// This function searches column id with column name from database
int selectColumnID(Database *pDB, char *pColumnName, int *pIDS, int nCount)
{
    char sColumns[DATA_MAX];
    snprintf(sColumns, sizeof(sColumns), "%s", pDB->sColumns);

    char *savePtr = NULL;
    int nCurrentID = 0;
    int i, nFound = 0;

    char *ptr = strtok_r(sColumns, ",", &savePtr);
    while (ptr != NULL)
    {
        // Check if column matches our search criteria
        if (strstr(ptr, pColumnName) != NULL) break;
        ptr = strtok_r(NULL, ",", &savePtr);
        nCurrentID++;
    }

    // Check if this column is already selected
    for (i = 0; i < nCount; i++)
    {
        if (pIDS[i] == nCurrentID)
        {
            nFound = 1;
            break;
        }
    }

    // Save selected column ID into array
    if (!nFound) pIDS[nCount++] = nCurrentID;
    return nCount;
}

// This function selects recordings from database with column id and appends those recordings in the pResponse variable
int selectWithID(const char *pFrom, int nID, int nFound, String *pResponse, int nDistinct)
{
    char sData[DATA_MAX];
    snprintf(sData, sizeof(sData), "%s", pFrom);

    char *savePtr = NULL;
    int nCurrentID = 0;

    char *ptr = strtok_r(sData, ",", &savePtr);
    while (ptr != NULL)
    {
        // Search current column in needed columns
        if (nID == nCurrentID)
        {
            char sColumn[DATA_MAX];
            while (*ptr == ' ') ptr++; // Remove spaces from front and back
            removeCharacter(sColumn, sizeof(sColumn), ptr, ' ');

            if (!nDistinct || strstr(pResponse->pData, sColumn) == NULL)
            {
                if (nFound) stringAppend(pResponse, ",", 1);
                else nFound = 1;

                // Append column name in response
                stringAppend(pResponse, sColumn, strlen(sColumn));
                break;
            }
        }

        ptr = strtok_r(NULL, ",", &savePtr);
        nCurrentID++;
    }

    return nFound;
}

// This function selects recordings from database with column id array and appends those recordings in the pResponse variable
int selectFromIDS(Database *pDB, int *pIDS, int nCount, String *pResponse, int nDistinct)
{
    if (!nCount) return 0;
    int i, nFound = 0;

    for (i = 0; i < nCount; i++) // Search current column in needed columns
        nFound = selectWithID(pDB->sColumns, pIDS[i], nFound, pResponse, nDistinct);

    if (nFound) stringAppend(pResponse, "\n", 1);
    int j, nRecordings = 0;

    for (i = 0; i < pDB->nRowCount; i++)
    {
        RowData *pRow = &pDB->pRows[i];
        nFound = 0;

        for (j = 0; j < nCount; j++) // Search current column in needed columns
            nFound = selectWithID(pRow->sData, pIDS[j], nFound, pResponse, nDistinct);

        if (nFound)
        {
            stringAppend(pResponse, "\n", 1);
            nRecordings += 1;
        }
    }

    return nRecordings;
}


// This function parses SQL queries and executing them according the query type
int executeSelectQuery(Database *pDB, char *pQuery, String *pResponse)
{
    // Get query length and validate
    int nLength = strlen(pQuery);
    if (!nLength) return -1; // return -1 means unsupported query

    int nRecordCount = 0;
    int nDistinct = 0;
    pQuery += 7; // Skip "SELECT" and space

    if (!strncmp(pQuery, "DISTINCT", 8))
    {
        pQuery += 9; // Skip "DISTINCT" and space
        nDistinct = 1;
    }

    // Select everything from database
    if (!strncmp(pQuery, "*", 1))
    {
        // Lock database for reading
        lockRead(&pDB->rwLock);

        int i, nColumnsDone = 0; // Iterate all comuns
        for (i = 0; i < pDB->nRowCount; i++)
        {
            if (!nColumnsDone) // Select columns first
            {
                stringAppend(pResponse, pDB->sColumns, strlen(pDB->sColumns));
                stringAppend(pResponse, "\n", 1);
                nColumnsDone = 1;
            }

            // Get next row
            RowData *pRow = &pDB->pRows[i];

            // Dont append row data if query is distinct and we have already similar data in response
            if (!nDistinct || strstr(pResponse->pData, pRow->sData) == NULL)
            {
                // Append recording into response
                stringAppend(pResponse, pRow->sData, strlen(pRow->sData));
                stringAppend(pResponse, "\n", 1);
                nRecordCount++;
            }
        }

        // Unlock database rwlock
        unlockRW(&pDB->rwLock);
        return nRecordCount;
    }
    else
    {
        if (strstr(pQuery, ",") != NULL)
        {
            char *savePtr = NULL;
            char *ptr = strtok_r(pQuery, ",", &savePtr);
            if (ptr == NULL) return -1;

            // Lock database for reading
            lockRead(&pDB->rwLock);

            // Column IDs which we want to select
            int nColumnIDs[pDB->nColumnCount];
            int nColumnCount = 0;

            while (ptr != NULL)
            {
                char sColumn[DATA_MAX];

                while (*ptr == ' ') ptr++; // Remove spaces from front and back
                removeCharacter(sColumn, sizeof(sColumn), ptr, ' ');

                // Select IDS from requested column;
                nColumnCount = selectColumnID(pDB, sColumn, nColumnIDs, nColumnCount);
                ptr = strtok_r(NULL, ",", &savePtr);
            }

            // Select recordings from column IDs
            nRecordCount = selectFromIDS(pDB, nColumnIDs, nColumnCount, pResponse, nDistinct);

            // Unlock database rwlock
            unlockRW(&pDB->rwLock);
            return nRecordCount;
        }
        else
        {
            char *savePtr = NULL;
            char *ptr = strtok_r(pQuery, " ", &savePtr);
            if (ptr == NULL) return -1;

            // Remove spaces from front
            while (*ptr == ' ') ptr++;

            int nColumnCount = 0;
            int nColumnIDs[1];

            // Lock database for reading
            lockRead(&pDB->rwLock);

            // Select IDS from requested column;
            nColumnCount = selectColumnID(pDB, ptr, nColumnIDs, 0);

            // Select recordings from column IDs
            nRecordCount = selectFromIDS(pDB, nColumnIDs, nColumnCount, pResponse, nDistinct);

            // Unlock database rwlock
            unlockRW(&pDB->rwLock);
            return nRecordCount;
        }
    }

    return -1;
}

typedef struct {
    char sColumn[DATA_MAX];
    char sValue[DATA_MAX];
    int nColumnID;
} UpdateSet;

// This function parses equality from the UPDATE sql query and saves parsed values into UpdateSet variable
int parseEquality(Database *pDB, UpdateSet *pSet, char *pData)
{
    char *savePtr, *savePtr2;
    char sData[DATA_MAX];

    while (*pData == ' ') pData++;
    removeCharacter(sData, sizeof(sData), pData, ' ');

    char *ptr = strtok_r(sData, "=", &savePtr);
    if (ptr == NULL) return 0;
    while (*ptr == ' ') ptr++;

    // Parse column name and remove spaces from back
    removeCharacter(pSet->sColumn, sizeof(pSet->sColumn), ptr, ' ');

    // Lock database for reading
    lockRead(&pDB->rwLock);

    // Mark colun IDS which we want to update
    int nColumnCount = 0;
    int nColumnIDs[1];

    // Select IDS from requested column;
    nColumnCount = selectColumnID(pDB, pSet->sColumn, nColumnIDs, 0);
    if (nColumnCount) pSet->nColumnID = nColumnIDs[0];

    // Unlock database rwlock
    unlockRW(&pDB->rwLock);

    ptr = strtok_r(NULL, "=", &savePtr);
    if (ptr == NULL) return 0;
    while (*ptr == ' ') ptr++;
    if (*ptr != '\'') return 0;

    ptr = strtok_r(ptr + 1, "'", &savePtr2);
    if (ptr == NULL) return 0;

    // Parse new value and remove whitespace 
    removeCharacter(pSet->sValue, sizeof(pSet->sValue), ptr, ' ');
    return 1;
}

// This function updates database recordings according to UpdateSet and UpdateSet condition
int updateDatabase(Database *pDB, UpdateSet *pSet, int nCount, UpdateSet *pCond)
{
    int i, nFound, nUpdatedCount = 0;
    for (i = 0; i < pDB->nRowCount; i++)
    {
        // Get next row
        RowData *pRow = &pDB->pRows[i];

        String data;
        stringInit(&data, DATA_MAX);

        // Get value from row wich we want to update
        nFound = selectWithID(pRow->sData, pCond->nColumnID, 0, &data, 0);

        // Check if row meets our condition
        if (nFound && !strncmp(data.pData, pCond->sValue, data.nUsed))
        {
            String rowData;
            stringInit(&rowData, DATA_MAX);

            char *savePtr;
            int nStarted = 0, nCurrID = 0;

            char *ptr = strtok_r(pRow->sData, ",", &savePtr);
            if (ptr == NULL) return 0;

            while (ptr != NULL)
            {
                if (nStarted) stringAppend(&rowData, ",", 1);
                else nStarted = 1;

                int j, nSetDone = 0;
                for (j = 0; j < nCount; j++)
                {
                    UpdateSet *pCurr = &pSet[j];
                    if (nCurrID == pCurr->nColumnID)
                    {
                        // Update value with a new one in row
                        stringAppend(&rowData, pCurr->sValue, strlen(pCurr->sValue));
                        nSetDone = 1;
                    }

                }

                // Append old value if this row is row updated
                if (!nSetDone) stringAppend(&rowData, ptr, strlen(ptr));
                else nUpdatedCount++;

                ptr = strtok_r(NULL, ",", &savePtr);
                nCurrID++;
            }

            // Update row
            snprintf(pRow->sData, sizeof(pRow->sData), "%s", rowData.pData);
            stringClear(&rowData); // Clear allocated string
        }
        
        // Clear allocated string
        stringClear(&data);
    }

    return nUpdatedCount;
}

// This function executes UPDATE query and appends response in the string
int executeUpdateQuery(Database *pDB, char *pQuery, String *pResponse)
{
    // Get query length and validate
    int nLength = strlen(pQuery);
    if (!nLength) return -1; // return -1 means unsupported query

    int nRecordCount = 0;
    pQuery += 7; // Skip "UPDATE" and space

    // We are supporting only TABLE SET query
    if (strncmp(pQuery, "TABLE SET", 9)) return -1;
    pQuery += 10; // Skip "TABLE SET" and space;

    UpdateSet setList[pDB->nColumnCount];
    UpdateSet condition;
    int nColumnCount = 0;

    // Parse condition from query
    char *pWhere = strstr(pQuery, "WHERE");
    if (pWhere == NULL) return -1;
    if (!parseEquality(pDB, &condition, pWhere + 6)) return -1;

    if (strstr(pQuery, ",") != NULL)
    {
        char *savePtr = NULL;
        char *ptr = strtok_r(pQuery, ",", &savePtr);
        if (ptr == NULL) return -1;

        while (ptr != NULL)
        {
            if (nColumnCount >= pDB->nColumnCount) break;

            // Initialize update instruction set
            UpdateSet *pSet = &setList[nColumnCount++];
            if (!parseEquality(pDB, pSet, ptr)) return -1;

            // Parse another column
            ptr = strtok_r(NULL, ",", &savePtr);
        }
    }
    else
    {
        char *savePtr = NULL;
        char *ptr = strtok_r(pQuery, " ", &savePtr);
        if (ptr == NULL) return -1;

        UpdateSet *pSet = &setList[nColumnCount++];
        if (!parseEquality(pDB, pSet, ptr)) return -1;
    }

    // Lock database for writing
    lockWrite(&pDB->rwLock);

    nRecordCount = updateDatabase(pDB, setList, nColumnCount, &condition);

    char sResponse[DATA_MAX]; // Create response
    int nLen = snprintf(sResponse, sizeof(sResponse), "Updated %d recordings", nRecordCount);
    if (nRecordCount) stringAppend(pResponse, sResponse, nLen);

    // Unlock database for writing
    unlockRW(&pDB->rwLock);
    return nRecordCount;
}

////////////////////////////////////////////////////////////////////////
// WORKER THREAD
////////////////////////////////////////////////////////////////////////

// This function initializes worker thread associated variables
void initWorker(WorkerContext *pCtx, int nID)
{
    // Init mutex variable
    if (pthread_mutex_init(&pCtx->mutex, NULL))
    {
        logToFile(ERROR, "Failed to init pthread mutex");
        exitFailure(NULL);
    }

    // Init condition variable
    if (pthread_cond_init(&pCtx->cond, NULL))
    {
        logToFile(ERROR, "Failed to init condition variable");
        pthread_mutex_destroy(&pCtx->mutex);
        exitFailure(NULL);
    }

    // Init worker thread related stuff
    pCtx->nInterrupt = 0;
    pCtx->nWorkerID = nID;
    pCtx->nClientFD = -1;
    pCtx->isInit = 1;
    pCtx->nBusy = 0;
}

// This function destrois worker thread associated variables
void destroyWorker(WorkerContext *pCtx)
{
    if (pCtx->isInit)
    {
        // Activate interrupt flag for worker thread
        lockMutex(&pCtx->mutex);
        pCtx->nInterrupt = 1;
        unlockMutex(&pCtx->mutex);

        // Wait thread to be interrupted
        signalCondition(&pCtx->cond);
        waitCondition(&pCtx->cond, &pCtx->mutex);

        // Destroy condition variable and mutex
        pthread_mutex_destroy(&pCtx->mutex);
        pthread_cond_destroy(&pCtx->cond);
        pCtx->isInit = 0;
    }

    // Close client connection if active
    if (pCtx->nClientFD >= 0)
    {
        close(pCtx->nClientFD);
        pCtx->nClientFD = -1;
    }
}

// This is the worker thread function
void* werkerThread(void *pArg)
{
    WorkerContext *pCtx = (WorkerContext*)pArg;
    logToFile(INFO, "Thread #%d: Waiting for connection", pCtx->nWorkerID);

    while (!pCtx->nInterrupt)
    {
        // Lock mutex and wait for activity
        lockMutex(&pCtx->mutex);
        waitCondition(&pCtx->cond, &pCtx->mutex);

        // Check if thread is interrupted by exit handler
        if (pCtx->nInterrupt)
        {
            unlockMutex(&pCtx->mutex);
            signalCondition(&pCtx->cond);
            break;
        }

        // Activate busy flag and unlock mutex
        pCtx->nBusy = 1;
        unlockMutex(&pCtx->mutex);

        // Check if worker has active connection
        if (pCtx->nClientFD >= 0)
        {
            char buffer[DATA_MAX];
            int nLen = read(pCtx->nClientFD, buffer, sizeof(buffer));
            if (nLen <= 0)
            {
                logToFile(ERROR,  "Can not read query from client");
                break;
            }

            int nStatus = -1;
            buffer[nLen] = '\0';
            logToFile(INFO, "Thread #%d: received query '%s'", pCtx->nWorkerID, buffer);
            
            String response; // Initialize response
            stringInit(&response, DATA_MAX);

            // Determine request type, parse query and send response to the clienrt
            if (!strncmp(buffer, "SELECT", 6)) nStatus = executeSelectQuery(&g_dataBase, buffer, &response);
            else if (!strncmp(buffer, "UPDATE", 6)) nStatus = executeUpdateQuery(&g_dataBase, buffer, &response);

            if (nStatus < 0) stringAppend(&response, "Invalid or unsupported query", 28);
            if (!response.nUsed) stringAppend(&response, "No recordings found for query", 29);
            else logToFile(INFO, "query completed, %d records have been returned.", nStatus < 0 ? 0 : nStatus);

            // Send response to the client
            if (write(pCtx->nClientFD, &nStatus, sizeof(nStatus)) < 0) break;
            if (write(pCtx->nClientFD, response.pData, response.nUsed) < 0) break;
            stringClear(&response); // Clear response

            // Close connection
            close(pCtx->nClientFD);
            pCtx->nClientFD = -1;

            // Sleep 0.5 econds to simulate intensive database execution
            usleep(500000);
        }

        // Mark worker as free
        lockMutex(&pCtx->mutex);
        pCtx->nBusy = 0;
        unlockMutex(&pCtx->mutex);

        // Notify main thread that worker is free
        signalCondition(&g_cond);
    }

    // Release any waiter sto this thread
    signalCondition(&g_cond);
    signalCondition(&pCtx->cond);

    return NULL;
}

////////////////////////////////////////////////////////////////////////
// Main stuff
////////////////////////////////////////////////////////////////////////

// This function parses command line arguments
void parseArgs(int argc, char *argv[], ServerConfig *pConf)
{
    int nOpt = 0, nCount = 0;

    while ((nOpt = getopt(argc, argv, "p:o:l:d:")) != -1) 
    {
        switch (nOpt)
        {
            case 'p':
                pConf->nPort = atoi(optarg);
                nCount++;
                break;
            case 'o':
                pConf->pLogFile = optarg;
                nCount++;
                break;
            case 'l':
                pConf->nPoolSize = atoi(optarg);
                nCount++;
                break;
            case 'd':
                pConf->pDBPath = optarg;
                nCount++;
                break;
            default:
                break;
        }
    }

    // Validate command line arguments
    if (nCount != 4 || pConf->nPoolSize < 2)
    {
        printf("Invalid or missing command line parameters\n");
        printf("Usage: %s -p PORT -o pathToLogFile –l poolSize –d datasetPath\n", argv[0]);
        exit(EXIT_FAILURE);
    }
}

// MAIN function
int main(int argc, char *argv[])
{
    // Init global stats
    g_logger.isInit = 0;
    g_workers.isInit = 0;
    g_dataBase.isInit = 0;

    // Intialize signal handler
    struct sigaction sigAct;
    sigemptyset(&sigAct.sa_mask);
    sigAct.sa_handler = exitHandler;
    sigAct.sa_flags = 0;

    // Register exit handler for SIGNINT signal
    if (sigaction(SIGINT, &sigAct, NULL) != 0)
        exitFailure("Failed to setup SIGINT handler");

    // Parse command line arguments
    ServerConfig config;
    parseArgs(argc, argv, &config);

    // Init Logger
    initMutex(&g_logger.mutex);
    g_logger.pPath = config.pLogFile;
    g_logger.isInit = 1;

    // Log parameters as requested
    logToFile(INFO, "Executing with parameters:");
    logToFile(INFO, "-p %d", config.nPort);
    logToFile(INFO, "-o %s", config.pLogFile);
    logToFile(INFO, "-l %d", config.nPoolSize);
    logToFile(INFO, "-d %s", config.pDBPath);

    // Run in background and detach from terminal
    // after this server will no longer own the shell
    if (daemon(0, 0) == -1) 
    {
        logToFile(ERROR, "Daemonization failed");
        exitFailure(NULL);
    }

    // Create listener socket
    g_nListenerSock = createServerSocket(config.nPort);

    // Load input dataset from tsv file
    initDatabase(&g_dataBase);
    loadDatabase(config.pDBPath, &g_dataBase);
    if (g_nInterrupted) exitFailure(NULL);

    // Init general mutex
    if (pthread_mutex_init(&g_mutex, NULL))
    {
        logToFile(ERROR, "Failed to init general pthread mutex");
        exitFailure(NULL);
    }

    // Init general condition variable
    if (pthread_cond_init(&g_cond, NULL))
    {
        logToFile(ERROR, "Failed to init general condition variable");
        pthread_mutex_destroy(&g_mutex);
        exitFailure(NULL);
    }

    // Mark global sync variables as initialized so we can 
    // destroy those variables later according to this flag
    g_syncInit = 1;

    // Initialize worker threads context
    WorkerContext threads[config.nPoolSize];
    g_workers.nWorkerCount = 0;
    g_workers.pWorkers = threads;
    int i;

    // Initialize and run worker threads
    for (i = 0; i < config.nPoolSize; i++)
    {
        // Check if interrupted
        if (g_nInterrupted) exitFailure(NULL);

        // Initialize current worker thread
        WorkerContext *pWorker = &threads[i];
        initWorker(pWorker, i);

        pthread_attr_t pattr;
        pthread_t threadId;

        // Run worker thread as detached
        if (pthread_attr_init(&pattr) ||
            pthread_attr_setdetachstate(&pattr, PTHREAD_CREATE_DETACHED) ||
            pthread_create(&threadId, &pattr, werkerThread, pWorker) ||
            pthread_attr_destroy(&pattr))
        {
            logToFile(ERROR, "Can not create worker thread");
            destroyWorker(pWorker);
            globalDestroy();
            return 1;
        }

        // Update global stat variables
        g_workers.nWorkerCount++;
        g_workers.isInit = 1;
    }

    // Main loop
    while (!g_nInterrupted)
    {
        socklen_t len = sizeof(struct sockaddr);
        struct sockaddr_in inAddr;
    
        /* Acceot to the new connection request */
        int nClientFD = accept(g_nListenerSock, (struct sockaddr*)&inAddr, &len);
        if (nClientFD < 0)
        {
            logToFile(ERROR, "Can not accept to the socket");
            break;
        }

        int nAssigned = 0;
        while (!nAssigned)
        {
            // Interate workers and check if any is not busy
            for (i = 0; i < g_workers.nWorkerCount; i++)
            {
                // Get workeker and lock
                WorkerContext *pWorker = &threads[i];
                lockMutex(&pWorker->mutex);

                // Check if worker is busy
                if (pWorker->nBusy)
                {
                    // This worker is busy, unlock and try another one
                    unlockMutex(&pWorker->mutex);
                    continue;
                }

                // At this case, found worker is not busy
                // we can use it for this connection
                pWorker->nClientFD = nClientFD;
                unlockMutex(&pWorker->mutex);

                // Notify worker about new connection
                signalCondition(&pWorker->cond);
                logToFile(INFO, "A connection has been delegated to thread id #%d", pWorker->nWorkerID);

                // Update assigned flag
                nAssigned = 1;
                break;
            }

            // At this case all workers are busy so we must
            // wait any worker thread to finish processing
            if (!nAssigned)
            {
                lockMutex(&g_mutex);
                // When worker finishes, it will send signal to 
                // this condition variable, so main will wake
                logToFile(INFO, "No thread is available! Waiting...");
                waitCondition(&g_cond, &g_mutex);
                unlockMutex(&g_mutex);
            }
        }
    }

    // Cleanup any allocared variable and exit
    globalDestroy();
    return 0;
}
