/**
 * @Author Ozan GECKIN
 *  1801042103
 */
#include <stdio.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/file.h>

typedef struct {
    pthread_t threadId;         // Students thread id
    sem_t semaphore;            // semaphore for synchronization
    sem_t *pSemNotify;          // semaphore to notify main about availability
    int nTerminated;            // thread is terminated or not

    char sName[NAME_MAX];       // name of the student
    int nQuality;               // quality value of this student 
    int nSpeed;                 // speed value of this student 
    int nCost;                  // cost value of this student

    char cHomework;             // homework this student is hired for
    int nSolved;                // homeworks solved by this student
    int nEarned;                // money earned by this student
    int nBusy;                  // flag to check this student is currently busy or not
} StudentThreadCtx;

typedef struct {
    const char *pHomework;      // homework file path
    char *pQueue;               // queue pointer for homeworks

    pthread_t threadId;         // H thread id
    int nTerminated;            // H thread is terminated by main
    int nFinished;              // H thread is finished working or not

    sem_t wait_sem;             // semaphore to wait for main thread
    sem_t semaphore;            // semaphore to notify main thread
} HThreadCtx;

static int h_money = 0;                 // remaninig money of H
static int nStudentCount = 0;           // We must save student count as public variable for gracefull termination

StudentThreadCtx *pStudents = NULL;     // We must save students array ponter as public for gracefull termination
HThreadCtx *pHThread = NULL;            // We must save H thread context as public for gracefull termination


void exitFailure(const char *pMessage);
void initSemaphore(sem_t *pSem);
void releaseSemaphore(sem_t *pSem);
void waitSemaphore(sem_t *pSem);
void unlockSemaphore(sem_t *pSem);
void threadSafeSet(int *pDst, int nSrc);
int threadSafeGet(int *pSrc);
void *studentThread(void *ctx);
void *thread_h(void *ctx);
void createThread(pthread_t *pThread, void *(*func)(void *), void *pArg, int nDtch);
int readFile(const char *pPath, char *pDst, int nSize);
int countLines(const char *pSrc, int nLength);
int createStudentThreads(StudentThreadCtx *pStudents, int nCount, char *pBuffemackear, int nLength, sem_t *pSem);
void terminateStudents(StudentThreadCtx *pStudents, int nCount);
void terminateHThread(HThreadCtx *pHThread);
StudentThreadCtx *findRelevantStudent(StudentThreadCtx *pStudents, int nCount, char cHomework);
int getMinCostStudent(StudentThreadCtx *pStudents, int nCount);
void exitHandler(int sig);


int main(int argc, char *argv[])
{
    if (argc < 4) { // Validate command line arguments and display usage if missing
        printf("Usage: %s homeworkFilePath studentsFilePath moneyAmount\n", argv[0]);
        printf("Example: %s homework students 10000\n", argv[0]);
        return 1;
    }

    // Intialize signal action handler
    struct sigaction sigAct;
    sigemptyset(&sigAct.sa_mask);
    sigAct.sa_flags = 0;
    sigAct.sa_handler = exitHandler;

    // Register exit handler for SIGNINT signal
    if (sigaction(SIGINT, &sigAct, NULL) != 0)
        exitFailure("Failed to setup SIGINT handler");

    char sStudents[8192]; // Load students file into the buffer
    int i, nLength = readFile(argv[2], sStudents, sizeof(sStudents));
    nStudentCount = countLines(sStudents, nLength);

    // Semaphore to wait students
    sem_t mainSemaphore;
    initSemaphore(&mainSemaphore);

    // Initialize students
    StudentThreadCtx students[nStudentCount];
    nStudentCount = createStudentThreads(students, nStudentCount, sStudents, nLength, &mainSemaphore);
    if (!nStudentCount) exitFailure("invalid or empty students file");

    printf("%d students-for-hire threads have been created.\nName Q S C\n", nStudentCount);
    pStudents = students; // Save H thread context in public variable for gracefull termination

    for (i = 0; i < nStudentCount; i++)
    {
        StudentThreadCtx *pStudent = &students[i];

        printf("%s %d %d %d\n", 
            pStudent->sName, pStudent->nQuality,
            pStudent->nSpeed, pStudent->nCost);
    }

    // Use char array as input queue
    char queue[LINE_MAX];
    queue[0] = '\0';

    // Initialize H thread context
    HThreadCtx h;
    h.nTerminated = 0;
    h.nFinished = 0;
    h.pHomework = argv[1];
    h.pQueue = queue;

    // Initialize semaphores
    initSemaphore(&h.wait_sem);
    initSemaphore(&h.semaphore);

    // Save H thread context in public variable for gracefull termination
    pHThread = &h; 

    // Initialzie H's money
    h_money = atoi(argv[3]);

    // Create and run POSIX thread for H (last argument 1 is for detache)
    createThread(&h.threadId, thread_h, &h, 1);
    int nResync = 1;

    while (1)
    {
        if (nResync) 
        {
            releaseSemaphore(&h.wait_sem);  // notify H thread to bring homework
            waitSemaphore(&h.semaphore);    // wait H thread for another homework
        }

        // No need sync if H thread is finished
        if (h.nFinished) nResync = 0;

        int length = strlen(queue);
        int nMoney = threadSafeGet(&h_money);

        // Check if H's money enough for lowest student cost
        if (nMoney < getMinCostStudent(students, nStudentCount))
        {
            printf("Money is over, closing.\n");
            break;  // H has no more money
        }
        else if (h.nFinished && !length)
        {
            printf("No more homeworks left or coming in, closing.\n");
            break; // H thread finished and no more homeworks in queue
        }
        else if (!length)
        {
            // Wait for homework in queue
            if (!h.nFinished) nResync = 1;
            continue;
        }

        // Get homework from queue
        char cHomework = queue[0];

        // Get student by priority
        StudentThreadCtx *pStudent = findRelevantStudent(students, nStudentCount, cHomework);
        if (pStudent == NULL) // all students are busy
        {
            waitSemaphore(&mainSemaphore);
            nResync = 0;
            continue;
        }

        // Pay to student from H's wallet
        nMoney -= pStudent->nCost;
        threadSafeSet(&h_money, nMoney);

        // Give student the homework
        pStudent->cHomework = cHomework;
        releaseSemaphore(&pStudent->semaphore);

        // Remove owned homework from queue
        for (i = 0; i < length; i++) queue[i] = queue[i+1];

        // Let H thread to bring another homework
        if (!h.nFinished) nResync = 1;
    }

    terminateHThread(pHThread);  // Terminate H thread if still running and cleanup H context
    terminateStudents(students, nStudentCount); // Terminate student threads and cleanup

    printf("Homeworks solved and money made by the students:\n");

    for (i = 0; i < nStudentCount; i++) // Print solved and earned statistics per student
        printf("%s %d %d\n", students[i].sName, students[i].nSolved, students[i].nEarned);

    sem_destroy(&mainSemaphore);
    return 0;
}
void exitFailure(const char *pMessage)
{
    fprintf(stderr, "%s: %s\n", pMessage, strerror(errno));
    exit(EXIT_FAILURE);
}

/*
    I was not allowed to use mutex, condition variables, additional files, even not signal so I decided to use semaphores 
    to ensure synchronization. I think semaphores are allowed because in PDF is written that anything else is fair game.
*/

void initSemaphore(sem_t *pSem)
{
    if (sem_init(pSem, 1, 0) == -1)
        exitFailure("Failed to init semaphore");
}

void releaseSemaphore(sem_t *pSem)
{
    if (sem_post(pSem) == -1) 
        exitFailure("Failed to post semaphore");
}

void waitSemaphore(sem_t *pSem)
{
    if (sem_wait(pSem) == -1) 
        exitFailure("Failed to wait semaphore");
}

void unlockSemaphore(sem_t *pSem)
{
    int value = 0;
    sem_getvalue(pSem, &value);
    if (value > 0) return;

    if (sem_post(pSem) == -1) 
        exitFailure("Failed to wait semaphore");
}

void threadSafeSet(int *pDst, int nSrc)
{
    // Use GCC atomic builtin operation to thread safe set the variable
    __sync_lock_test_and_set(pDst, nSrc);
}

int threadSafeGet(int *pSrc)
{
    // Use GCC atomic builtin operation to thread safe read the variable
    return __sync_add_and_fetch(pSrc, 0);
}

void *studentThread(void *ctx)
{
    /* Cast thread argument to StudentThreadCtx variable */
    StudentThreadCtx *pCtx = (StudentThreadCtx*)ctx;

    while (1) 
    {
        printf("%s is waiting for a homework\n", pCtx->sName);

        // Wait main thread for homework assignment
        waitSemaphore(&pCtx->semaphore);
        threadSafeSet(&pCtx->nBusy, 1);

        // Check if main thread terminated
        if (pCtx->nTerminated) break;

        printf("%s is solving homework %c for %d, H has %dTL left.\n", 
            pCtx->sName, pCtx->cHomework, pCtx->nCost, threadSafeGet(&h_money));

        // Sleep to simulate homework solving
        int x = 6 - pCtx->nSpeed;
        sleep(x);

        // Increase counters
        pCtx->nEarned += pCtx->nCost;
        pCtx->nSolved++;

        // Notify main thread about ready for another homework
        threadSafeSet(&pCtx->nBusy, 0);
        unlockSemaphore(pCtx->pSemNotify);
    }

    // cleanup this student context
    sem_destroy(&pCtx->semaphore);
    pthread_exit(NULL);
}

void *thread_h(void *ctx)
{
    // Cast thread argument to GThread context
    HThreadCtx *pCtx = (HThreadCtx*)ctx;
    char *pQueue = pCtx->pQueue;

    // Open homeworks file for reading only
    int nByte, nFD = open(pCtx->pHomework, O_RDONLY, 0644);
    if (nFD < 0) exitFailure("Failed to open homework file");

    char sHomework[2];
    sHomework[1] = '\0';
    int nDoWait = 1;

    while (1)
    {
        // Wait main thread for access
        if (nDoWait) waitSemaphore(&pCtx->wait_sem);
        nDoWait = 1;

        // G thread is terminated by main
        if (pCtx->nTerminated) break;

        // Read homework from input file
        nByte = read(nFD, &sHomework[0], 1);

        // Check if no more homework or money is over
        if (nByte <= 0 || !threadSafeGet(&h_money))
        {
            pCtx->nFinished = 1;
            break;
        }

        // Ignore new line character in input file
        if (sHomework[0] == '\n')
        {
            nDoWait = 0;
            continue;
        }

        strcat(pQueue, sHomework); // Add homework to queue
        printf("H has a new homework %s; remaining money is %dTL\n", sHomework, threadSafeGet(&h_money));

        // Notify main thread about new homework in queue
        releaseSemaphore(&pCtx->semaphore);
    }

    if (nByte <= 0) printf("H has no other homeworks, terminating.\n");
    else printf("H has no more money for homeworks, terminating.\n");

    // Close file descriptor
    close(nFD);

    // Notify main thread about completion
    releaseSemaphore(&pCtx->semaphore);
    return NULL;
}

void createThread(pthread_t *pThread, void *(*func)(void *), void *pArg, int nDtch)
{
    // Create and run POSIX thread
    if (pthread_create(pThread, NULL, func, pArg))
        exitFailure("Thread start failed");

    // Make thread detached if required
    if (nDtch) pthread_detach(*pThread);
}

int readFile(const char *pPath, char *pDst, int nSize)
{
    int nFD = open(pPath, O_RDONLY, 0644);
    if (nFD < 0) exitFailure("Failed to open students file");

    int nBytes = read(nFD, pDst, nSize);
    if (nBytes <= 0)
    {
        close(nFD); // failed to read from file
        exitFailure("Failed to read students file");
    }

    // Close student file descriptor
    close(nFD);

    // Null terminate read buffer
    pDst[nBytes] = '\0';
    return nBytes;
}

int countLines(const char *pSrc, int nLength)
{
    int i, nCount = 0, nLastPosit = 0;

    // Count new line characters
    for (i = 0; i < nLength; i++)
    {
        if (pSrc[i] == '\n')
        {
            nLastPosit = i;
            nCount++;
        }
    }

    // Check if last line doesnot contain new line character
    if ((i - nLastPosit > 1) && pSrc[i] != '\n') nCount++;

    return nCount;
}

int createStudentThreads(StudentThreadCtx *pStudents, int nCount, char *pBuffer, int nLength, sem_t *pSem)
{
    char *pOffset = pBuffer;
    int i, nStudents = 0;

    for (i = 0; i < nCount; i++)
    {
        char *pEnd = strstr(pOffset, "\n");
        int nLength = (pEnd) ? pEnd - pOffset : 0;
        if (!nLength) break;

        char sLine[nLength + 1];
        strncpy(sLine, pOffset, nLength);
        sLine[nLength] = '\0';
        pOffset = pEnd + 1;

        // Scan student parameters from line
        StudentThreadCtx *pStudent = &pStudents[i];
        int nCount = sscanf(sLine, "%s %d %d %d", 
            pStudent->sName, &pStudent->nQuality,
            &pStudent->nSpeed, &pStudent->nCost);

        // Invalid or finished input
        if (nCount != 4) break;

        // initialize student thread sync context
        initSemaphore(&pStudent->semaphore);
        pStudent->pSemNotify = pSem;
        pStudent->nTerminated = 0;
        pStudent->cHomework = 0;
        pStudent->nEarned = 0;
        pStudent->nSolved = 0;
        pStudent->nBusy = 0;

        // initialize and run this student thread
        createThread(&pStudent->threadId, studentThread, pStudent, 0);
        nStudents++;
    }

    return nStudents;
}

void terminateStudents(StudentThreadCtx *pStudents, int nCount)
{
    int i;
    for (i = 0; i < nCount; i++)
    {
        StudentThreadCtx *pStudent = &pStudents[i];     // get student from students array
        pStudent->nTerminated = 1;                      // set termination flag for student
        releaseSemaphore(&pStudent->semaphore);         // wake student to read this termination flag
        pthread_join(pStudent->threadId, NULL);         // join to the thread (actually wait until thread is terminated)
    }
}

void terminateHThread(HThreadCtx *pHThread)
{
    if (!pHThread->nFinished)
    {
        pHThread->nTerminated = 1;
        releaseSemaphore(&pHThread->wait_sem);  // notify H thread about termination
        waitSemaphore(&pHThread->semaphore);    // wait H thread to be terminated
    }

    // G thread is terminated, destroy context
    sem_destroy(&pHThread->wait_sem);
    sem_destroy(&pHThread->semaphore);
}

StudentThreadCtx *findRelevantStudent(StudentThreadCtx *pStudents, int nCount, char cHomework)
{
    int i, nFoundVal = 0;
    StudentThreadCtx *pFound = NULL;

    for (i = 0; i < nCount; i++)
    {
        // Check this student is busy or not
        StudentThreadCtx *pStudent = &pStudents[i];
        if (threadSafeGet(&pStudent->nBusy)) continue;

        // Check if student fits H's budget
        int nMoneyG = threadSafeGet(&h_money);
        if (nMoneyG < pStudent->nCost) continue;

        if (cHomework == 'Q') // find student for quality
        {
            if (nFoundVal < pStudent->nQuality)
            {
                nFoundVal = pStudent->nQuality;
                pFound = pStudent;
            }
        }
        else if (cHomework == 'S') // find student for speed
        {
            if (nFoundVal < pStudent->nSpeed)
            {
                nFoundVal = pStudent->nSpeed;
                pFound = pStudent;
            }
        }
        else if (cHomework == 'C') // find student for low cost
        {
            if (!nFoundVal || nFoundVal > pStudent->nCost)
            {
                nFoundVal = pStudent->nCost;
                pFound = pStudent;
            }
        }
    }

    return pFound;
}

int getMinCostStudent(StudentThreadCtx *pStudents, int nCount)
{
    int i, nMinCost = 0;

    for (i = 0; i < nCount; i++)
    {
        StudentThreadCtx *pStudent = &pStudents[i];

        if (!nMinCost || nMinCost > pStudent->nCost) 
            nMinCost = pStudent->nCost;
    }

    return nMinCost;
}

void exitHandler(int sig)
{
    if (sig == SIGINT)
        printf("Termination signal received, closing.\n");

    // Gracefull termination
    terminateHThread(pHThread);
    terminateStudents(pStudents, nStudentCount);    

    exit(0);
}
