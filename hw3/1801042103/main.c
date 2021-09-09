#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <semaphore.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

/*
 * The maximum length of string that contains the name of opened FIFOs
 */
#define FIFO_NAMES_MAX 512 * 512

/*
 * Maximum length of string
 */
#define STR_MAX 512

/*
 * The structure that resides inside Shared Memory
 */
struct SharedMemory
{
    sem_t sem;
    pid_t pid;
    int num_potatoes;
    char fifo_name[FIFO_NAMES_MAX];
    long num_cool_down;
};

/*
 * Enumeration for type of message, either I send a potato or exit message
 */
enum MessageType
{
    POTATO, EXIT
};

/*
 * Structure read from and send to FIFO
 */
struct Message
{
    enum MessageType type;
    int pid;
};

static struct SharedMemory *p_shared_memory;
char **fifo_names = NULL;
int fifo_names_length = 0;
int fifo_open_index;
sem_t *nameSemaphore;

void ctrl_c(int signal);
void send_exit_to_all(char **fifo_names, int fifo_names_length, int current_fifo_index);
void send_potato(char **fifo_names, int fifo_names_length, int current_fifo_index, int current_cool_down, int is_first);
int open_one_fifo(char **fifo_names, int fifo_names_length);
struct SharedMemory *open_shared_memory(const char *shared_memory_name);

int main(int argc, const char *argv[])
{
    signal(SIGINT, ctrl_c);
    char name_of_shared_memory[STR_MAX] = { 0 };
    char file_with_fifo_names[STR_MAX] = { 0 };
    char name_of_semaphore[STR_MAX] = { 0 };
    long N = -1;

    int c;
    char *temp;

    /*
     * Get arguments using getopt
     */
    while ((c = getopt(argc, (char *const *) argv, ":b:s:f:m:")) != -1)
    {
        switch (c)
        {
            case 'b':

                N = strtol(optarg, &temp, 10);
                if (temp == optarg)
                {
                    printf("Invalid number: %s\n", optarg);
                    printf("./executable –b haspotatoornot –s nameofsharedmemory –f filewithfifonames -m namedsemaphore\n");
                    return -1;
                }
                break;
            case 's':
                strcpy(name_of_shared_memory, optarg);
                break;
            case 'f':
                strcpy(file_with_fifo_names, optarg);
                break;
            case 'm':
                strcpy(name_of_semaphore, optarg);
                break;
            default:
                printf("Invalid argument found: %c\n", optopt);
                printf("./executable –b haspotatoornot –s nameofsharedmemory –f filewithfifonames -m namedsemaphore\n");
                return -2;
        }
    }

    /*
     * Verify all arguments
     */
    if (strlen(name_of_shared_memory) == 0 || strlen(file_with_fifo_names) == 0 || strlen(name_of_semaphore) == 0 || N == -1)
    {
        printf("./executable –b haspotatoornot –s nameofsharedmemory –f filewithfifonames -m nameofsemaphore\n");
        return -3;
    }

    /*
     * Open file containing FIFO names
     */
    FILE *file = fopen(file_with_fifo_names, "r");
    if (file == NULL)
    {
        printf("Cannot open file: %s\n", file_with_fifo_names);
        return -4;
    }
    
    /*
     * open name semaphore 
     */
    nameSemaphore=sem_open(name_of_semaphore,O_CREAT,0666,1);
   
    if(nameSemaphore==SEM_FAILED){//check name semaphore open
          perror("nameSemaphore");
          return -5;
    }

    /*
     * Keep reading lines inside, and add to `fifo_names`
     */
    while (true)
    {
        char buffer[STR_MAX] = { 0 };
        if (fgets(buffer, STR_MAX - 1, file) == NULL) break;

        if (buffer[strlen(buffer) - 1] == '\n') buffer[strlen(buffer) - 1] = '\0';
        if (buffer[strlen(buffer) - 1] == '\r') buffer[strlen(buffer) - 1] = '\0';

        fifo_names = (char **)realloc(fifo_names, sizeof(char *) * (fifo_names_length + 1));
        if (fifo_names == NULL)
        {
            perror("realloc");
            return -5;
        }

        fifo_names[fifo_names_length] = (char *)malloc(strlen(buffer) + 1);
        if (fifo_names[fifo_names_length] == NULL)
        {
            perror("realloc");
            return -5;
        }

        strcpy(fifo_names[fifo_names_length], buffer);
        fifo_names_length++;
    }
    fclose(file);

    /*
     * Open the shared memory segment
     * Open one FIFO and attach to current process
     */
    p_shared_memory = open_shared_memory(name_of_shared_memory);
    fifo_open_index = open_one_fifo(fifo_names, fifo_names_length);
    if (fifo_open_index == -1)
    {
        printf("Cannot open FIFO\n");
        return -6;
    }

    /*
     * Send a potato if needed
     */
    if (N > 0)
    {
        send_potato(fifo_names, fifo_names_length, fifo_open_index, (int)N, 1);
    }

    /*
     * Keep reading message, decreasing its cool down and resend to random FiFO
     */
    while (true)
    {
        int fifo = open(fifo_names[fifo_open_index], O_RDWR);
        struct Message message;
        read(fifo, &message, sizeof(message));
        if (message.type == EXIT) break;
        close(fifo);

        printf("pid=%ld receiving potato number %ld from %s\n",
               (long)getpid(), (long)message.pid, fifo_names[fifo_open_index]);

        sem_wait(nameSemaphore);
        if (p_shared_memory->num_cool_down > 0) --p_shared_memory->num_cool_down;
        long new_N = p_shared_memory->num_cool_down;
        if (new_N == 0)
        {
            if (p_shared_memory->num_potatoes > 0) --p_shared_memory->num_potatoes;
        }
        long new_num_potatoes = p_shared_memory->num_potatoes;
        sem_post(nameSemaphore);

        if (new_N == 0)
        {
            printf("pid=%ld; potato number %ld has cooled down.\n", (long)getpid(), (long)message.pid);
        }
        else
        {
            send_potato(fifo_names, fifo_names_length, fifo_open_index, (int)new_N, 0);
        }

        if (new_num_potatoes == 0)
        {
            send_exit_to_all(fifo_names, fifo_names_length, fifo_open_index);
            break;
        }
    }

    /*
     * Free the shared memory and unlink it (shm_unlink will wait for last process to unlink before actually unlinking)
     */
    munmap(p_shared_memory, sizeof(struct SharedMemory));
    shm_unlink(name_of_shared_memory);
    sem_close(nameSemaphore);
    for (int i = 0; i < fifo_names_length; i++) free(fifo_names[i]);
    free(fifo_names);

    return 0;
}
/**
 * Check cntrl-c
 */
void ctrl_c(int signal)
{
    printf("Ctrl-c detected\n");
    for (int i = 0; i < fifo_names_length; i++)
    {
        if (i == fifo_open_index) continue;

        int d = open(fifo_names[i], O_RDWR);
        if (d == -1) continue;

        struct Message message;
        message.pid = -1;
        message.type = EXIT;
        write(d, &message, sizeof(message));
    }
    exit(0);
}
/*
 * Send exit message to all FIFOs
 */
void send_exit_to_all(char **fifo_names, int fifo_names_length, int current_fifo_index)
{
    for (int i = 0; i < fifo_names_length; i++)
    {
        if (i == current_fifo_index) continue;

        int fd = open(fifo_names[i], O_RDWR);
        if (fd != -1)
        {
            struct Message message;
            message.type = EXIT;
            message.pid = -1;
            write(fd, &message, sizeof(message));
            close(fd);
        }

    }
}
/*
 * Randomly select a FIFO from fifo_names and send the potato to it with `current_cool_down` cool down
 *
 * If it is first time sending the potato (specified by is_first), increase potato counter inside shared memory
 */
void send_potato(char **fifo_names, int fifo_names_length, int current_fifo_index, int current_cool_down, int is_first)
{
    int fd, other_end_index;
    while (true)
    {
        int i = ((int)rand()) % fifo_names_length;
        if (i == current_fifo_index) continue;

        fd = open(fifo_names[i], O_RDWR);
        if (fd != -1)
        {
            other_end_index = i;
            break;
        }
    }

    sem_wait(&p_shared_memory->sem);
    printf("pid=%ld sending potato number %ld to %s; this is switch number %d\n",
           (long)getpid(), (long)p_shared_memory->pid, fifo_names[other_end_index], current_cool_down);

    p_shared_memory->pid = getpid();
    p_shared_memory->num_cool_down = current_cool_down;

    if (is_first)
        p_shared_memory->num_potatoes++;

    struct Message message;
    message.type = POTATO;
    message.pid = getpid();
    write(fd, &message, sizeof(message));

    sem_post(&p_shared_memory->sem);
}
/*
 * Go through all FIFO names and open the first that is already not opened (specified by fifo_name)
 */
int open_one_fifo(char **fifo_names, int fifo_names_length)
{
    for (int i = 0; i < fifo_names_length; i++)
    {
        mkfifo(fifo_names[i], 0666);
    }

    for (int i = 0; i < fifo_names_length; i++)
    {
        if (strstr(p_shared_memory->fifo_name, fifo_names[i]) == NULL)
        {
            sem_wait(&p_shared_memory->sem);
            strcat(p_shared_memory->fifo_name, fifo_names[i]);
            sem_post(&p_shared_memory->sem);

            return i;
        }
    }
    return -1;
}

/*
 * Open the shared memory whose name is specified by `shared_memory_name`
 *
 * If it is just created also initialize necessary variable inside it
 */
struct SharedMemory *open_shared_memory(const char *shared_memory_name)
{
    int made = 1;
    int pid = shm_open(shared_memory_name, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
    if (pid == -1 && errno == EEXIST)
    {
        made = 0;
        pid = shm_open(shared_memory_name, O_RDWR, S_IRUSR | S_IWUSR);
    }

    if (pid == -1)
    {
        perror("shm_open");
        exit(EXIT_FAILURE);
    }

    if (made == 1)
    {
        if (ftruncate(pid, sizeof(struct SharedMemory)) == -1)
        {
            perror("ftruncate");
            exit(EXIT_FAILURE);
        }
    }

    struct SharedMemory *result = (struct SharedMemory *)mmap(NULL, sizeof(struct SharedMemory), PROT_READ | PROT_WRITE, MAP_SHARED, pid, 0);

    if (result == NULL)
    {
        perror("mmap");
        exit(EXIT_FAILURE);
    }
    if (made == 1)
    {
        if (sem_init(&result->sem, 1, 1) == -1)
        {
            perror("sem_init");
            shm_unlink(shared_memory_name);
            exit(EXIT_FAILURE);
        }

        strcpy(result->fifo_name, "");
        result->num_potatoes = 0;
    }
    return result;
}