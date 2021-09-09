/*
	Autor = Ozan GEÇKIN
	1801042103
 */
#ifndef HELPER_H
#define HELPER_H
#include <stdio.h>
#include <semaphore.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <fcntl.h>

/*
	Global mutexes used accross different c files
*/
extern sem_t * bufmutex; //Mutex for buffer used by vaccinator and nurse
extern sem_t * citmutex; //Mutex for citizen data used by vaccinator and citizen
extern sem_t * filemutex; //Mutex for file read used by nurse
extern sem_t * vacmutex; //Mutex for citizen to citizen to exchange information about the number of vaccinations

extern sem_t * empty; //posted by nurse(producer) and waited by vaccinator(consumer)
extern sem_t * full; //receive by vaccinator(consumer) and waiting by nurse(producer)

typedef struct{
	int pid;
	int shot_number;
}citizen_struct;

typedef struct{
	char * buffer; //This is the data buffer shared between nurse processes and vaccinator processes
	int buffer_size; //Buffer size
	
	int v1_ctr; //Vaccine 1 counter in the buffer
	int v2_ctr; //Vaccine 2 counter in the buffer
	
	int file_read_all_flag;
}buf_ipc_data_struct;

typedef struct{
	citizen_struct * citizens;
	int citizens_buffer_len;
	int vaccinated_citizens;

	int number_of_citizens;
	int number_of_shots;
}cit_ipc_data_struct;


typedef struct{
	int no_vaccination;
	int kill_process;
}vac_ipc_data_struct;



extern buf_ipc_data_struct * buf_ipc_data;
extern cit_ipc_data_struct * cit_ipc_data;
extern vac_ipc_data_struct * vac_ipc_data;


void nurse(const int no, const int filedesc,const int number_of_shots, const int number_of_citizens, int * nurses_shared_counter);
void citizen(const int no, const int t);
void vaccinator(const int no, const int number_of_shots, const int number_of_citizens);

#endif