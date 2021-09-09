/*
	Autor = Ozan GEÇKIN
	1801042103
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <semaphore.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "helper.h"


int * nurses_shared_counter = NULL;
buf_ipc_data_struct * buf_ipc_data = NULL;
cit_ipc_data_struct * cit_ipc_data = NULL;
vac_ipc_data_struct * vac_ipc_data = NULL;
sem_t * bufmutex;
sem_t * citmutex;
sem_t * vacmutex;
sem_t * empty;
sem_t * full;
sem_t * filemutex;

typedef struct{
	int n;
	int v;
	int c;
	int t;
	int b;
	char * file_path;
} command_line_arg_struct;

command_line_arg_struct prog_args = {0};
int in_file_desc = -1;
void program_exit_function(int status, void * arg);
void print_program_usage();
int validate_command_line_args(int argc, const char *argv[], command_line_arg_struct * prog_args);
void sig_handler(int sig);

int main(int argc, char const *argv[]){

	//Register the sig_handle for SIGNINT from terminal CTRL-C
	signal(SIGINT,sig_handler); 
	//For each process exit we clean the memory in this function
	on_exit(program_exit_function,NULL);


	//This structure is helper to get all arguments from the command line parameters
	
	validate_command_line_args(argc,argv,&prog_args);

	int number_of_nurses = prog_args.n;
	int number_of_vaccinators = prog_args.v;
	int number_of_citizens = prog_args.c;
	int number_of_shots = prog_args.t;
	int buffer_size = prog_args.b;
	
	printf("Welcome to the GTU344 clinic. Number of citizens to vaccinate c=%d with t=%d doses.\n",number_of_citizens,number_of_shots);

	
	bufmutex = sem_open ("bufmutex", O_CREAT | O_EXCL, 0644, 1); //bufmutex must be binary 1 or 0
	if(bufmutex == NULL){
		fprintf(stderr,"'Bufmutex' semaphore cannot be created\n");
		exit(EXIT_FAILURE);
	}
	citmutex = sem_open ("citmutex", O_CREAT | O_EXCL, 0644, 1); //citmutex must be binary 1 or 0
	if(citmutex == NULL){
		fprintf(stderr,"'citmutex' semaphore cannot be created\n");
		exit(EXIT_FAILURE);
	}
	full = sem_open ("full", O_CREAT | O_EXCL , 0644, 0); //Since our buffer is empty full means no data
	if(full == NULL){
		fprintf(stderr,"'full' semaphore cannot be created\n");
		exit(EXIT_FAILURE);
	}
	empty = sem_open ("empty", O_CREAT | O_EXCL, 0644, buffer_size); //We have buffersize+1 empty slots
	if(empty == NULL){
		fprintf(stderr,"'empty' semaphore cannot be created\n");
		exit(EXIT_FAILURE);
	}
	vacmutex = sem_open ("vacmutex", O_CREAT | O_EXCL, 0644, 1); //We have buffersize+1 empty slots
	if(vacmutex == NULL){
		fprintf(stderr,"'vacmutex' semaphore cannot be created\n");
		exit(EXIT_FAILURE);
	}
	filemutex = sem_open ("filemutex", O_CREAT | O_EXCL, 0644, 1); //filemutex must be binary 1 or 0
	if(filemutex == NULL){
		fprintf(stderr,"'filemutex' semaphore cannot be created\n");
		exit(EXIT_FAILURE);
	}


    buf_ipc_data = (buf_ipc_data_struct*)mmap ( NULL, sizeof(buf_ipc_data_struct),
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_ANONYMOUS,
            0, 0 );
    if(buf_ipc_data == MAP_FAILED){
        printf("Mapping Failed\n");
        return EXIT_FAILURE;
    }
	
	buf_ipc_data->buffer_size = buffer_size;
	buf_ipc_data->v1_ctr = 0;
	buf_ipc_data->v2_ctr = 0;
	buf_ipc_data->file_read_all_flag = 0;

	buf_ipc_data->buffer = (char*)mmap ( NULL, buffer_size*sizeof(char),
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_ANONYMOUS,
            0, 0 );
    if(buf_ipc_data->buffer == MAP_FAILED){
        printf("Mapping Failed\n");
        return EXIT_FAILURE;
    }

    cit_ipc_data = (cit_ipc_data_struct*)mmap ( NULL, sizeof(cit_ipc_data_struct),
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_ANONYMOUS,
            0, 0 );
    if(cit_ipc_data == MAP_FAILED){
        fprintf(stderr,"cit ipda data mapping Failed\n");
        return EXIT_FAILURE;
    }
	cit_ipc_data->citizens = (citizen_struct*)mmap ( NULL, number_of_citizens*sizeof(citizen_struct),
			PROT_READ | PROT_WRITE,
			MAP_SHARED | MAP_ANONYMOUS,
			0, 0 );
	if(cit_ipc_data->citizens == MAP_FAILED){
		printf("Mapping Failed\n");
		return EXIT_FAILURE;
	}

	cit_ipc_data->citizens_buffer_len = 0;
	cit_ipc_data->vaccinated_citizens = 0;
	cit_ipc_data->number_of_citizens = number_of_citizens;
	cit_ipc_data->number_of_shots = number_of_shots;


	nurses_shared_counter = (int*)mmap ( NULL, sizeof(nurses_shared_counter),
		PROT_READ | PROT_WRITE,
		MAP_SHARED | MAP_ANONYMOUS,
		0, 0 );
	if( nurses_shared_counter == MAP_FAILED){
		printf("Mapping Failed\n");
		return EXIT_FAILURE;
	}

	//Used for nurse and reverse counted
	*nurses_shared_counter = 2*number_of_citizens*number_of_shots;


	vac_ipc_data = (vac_ipc_data_struct*)mmap ( NULL, sizeof(vac_ipc_data_struct),
		PROT_READ | PROT_WRITE,
		MAP_SHARED | MAP_ANONYMOUS,
		0, 0 );
	if( vac_ipc_data == MAP_FAILED){
		printf("Mapping Failed\n");
		return EXIT_FAILURE;
	}
	vac_ipc_data->no_vaccination = 0;
	vac_ipc_data->kill_process = 0;


	in_file_desc = open(prog_args.file_path, O_RDONLY);
	if(in_file_desc == -1){
		fprintf(stderr,"Cannot open file %s\n",prog_args.file_path);
		exit(EXIT_FAILURE);
	}
	free(prog_args.file_path); //Clean the allocated file name
	
	int i;
	int number_of_child_processes = number_of_nurses + number_of_vaccinators + number_of_citizens;

	//Now fork the processes

    for (i = 0; i < number_of_nurses; i++){
		int pid = fork(); 
		if (pid == 0){
			nurse(i+1,in_file_desc, number_of_shots, number_of_citizens, nurses_shared_counter);
			exit(EXIT_SUCCESS);
		}
		else if(pid == -1){
			fprintf(stderr,"Cannot forked nurse process");
			exit(EXIT_FAILURE);
		}
	}

    for (i = 0; i < number_of_vaccinators; i++){
		int pid = fork();
		if (pid == 0){
			vaccinator(i+1,number_of_shots, number_of_citizens);
			exit(EXIT_SUCCESS);
		}
		else if(pid == -1){
			fprintf(stderr,"Cannot forked vaccinator process");
			exit(EXIT_FAILURE);
		}
	}

	for (i = 0; i < number_of_citizens; i++){
		int pid = fork();
		if (pid == 0){
			citizen(i+1, number_of_shots);
			exit(EXIT_SUCCESS);
		}
		else if(pid == -1){
			fprintf(stderr,"Cannot forked citizen process");
			exit(EXIT_FAILURE);
		}
	}

	//Only the parent process is here! Now it must wait to close 
	for(i = 0 ; i<number_of_child_processes; ++i){
		//Wait all child processes to close
		wait(NULL);
	}

	printf("All citizens have been vaccinated.\n");
	//No need to enter critical region since only parent can enter here and all the child processes are dead
	printf("The clinic is now closed. Stay healthy.\n");

	//Now close the file, clean up the memory
	close(in_file_desc);
	munmap(cit_ipc_data->citizens, number_of_citizens*sizeof(citizen_struct));
	munmap(cit_ipc_data,sizeof(cit_ipc_data_struct));
	munmap(buf_ipc_data->buffer, buffer_size*sizeof(char));
	munmap(buf_ipc_data,sizeof(buf_ipc_data_struct));
	munmap(nurses_shared_counter,sizeof(int));
	munmap(vac_ipc_data,sizeof(vac_ipc_data_struct));

	return EXIT_SUCCESS;
}


void program_exit_function(int status, void * arg){

	//suppress the warning
	(void)status;
	(void)arg; 

	sem_close(bufmutex);
	sem_close(citmutex);
	sem_close(vacmutex);
	sem_close(full);
	sem_close(empty);
	sem_close(filemutex);

	sem_unlink("bufmutex");
	sem_unlink("citmutex");
	sem_unlink("vacmutex");

	sem_unlink("empty");
	sem_unlink("full");

	sem_unlink("filemutex");

}
void print_program_usage(){
	fprintf(stderr, "Usage [-nvcbti] [args...]\n");
	fprintf(stderr, "Nurse number : -n >= 2\n");
	fprintf(stderr, "Vaccinator number : -n >= 2\n");
	fprintf(stderr, "Citizen number : -n >= 2\n");
	fprintf(stderr, "Number of shots : -t >= 1\n");
	fprintf(stderr, "Buffer size : -b >= t*c+1\n");
}
int validate_command_line_args(int argc, const char *argv[], command_line_arg_struct * prog_args){

	if(argc < 13){
		fprintf(stderr,"Wrong number of input arguments\n");
		print_program_usage();
		exit(EXIT_FAILURE);
	}

	if(prog_args == NULL){
		fprintf(stderr,"Program argument struct is NULL\n");
		exit(EXIT_FAILURE);
	}

	int ret = EXIT_SUCCESS;
	int i;
	prog_args->file_path = NULL;

	//Start from 1, skip the program name which is the first argument
	for(i = 1; i<argc && ret == EXIT_SUCCESS; ++i){

		//Validate the dashline
		if(argv[i][0] != '-'){
			//Error print usage
			print_program_usage();
			ret = 0;
			break;
		}
		
		char c = argv[i][1]; //type

		i++;//Get the argument
		if(i > argc){
			fprintf(stderr,"Argument can't found after '%c'\n",c);
			ret = EXIT_FAILURE;
			break;
		}
		const char * oparg = (argv[i]);

		switch(c){
			case 'n':{
				prog_args->n = atoi(oparg);
				if(prog_args->n < 2){
					fprintf(stderr,"Nurse number cannot be smaller than 2, given %d\n",prog_args->n);
					ret = EXIT_FAILURE;
				}
				break;
			}
			case 'v':{
				prog_args->v = atoi(oparg);
				if(prog_args->v < 2){
					fprintf(stderr,"Vaccinator number cannot be smaller than 2, given %d\n",prog_args->v);
					ret = EXIT_FAILURE;
				}
				break;
			}
			case 'c':{
				prog_args->c = atoi(oparg);
				if(prog_args->c < 3){
					fprintf(stderr,"Citizen number cannot be smaller than 3, given %d\n",prog_args->c);
					ret = EXIT_FAILURE;
				}
				break;
			}
			case 'b':{
				int b = atoi(oparg);
				prog_args->b = b;
				//Here we cannot validate buffer size directly but after getting all the args we will validate!
				break;
			}
			case 't':{
				prog_args->t = atoi(oparg);
				if(prog_args->t < 1){
					fprintf(stderr,"Number of shots  cannot be smaller than 1, given %d\n",prog_args->t);
					ret = EXIT_FAILURE;
				}
				break;
			}
			case 'i':{
				const char * s = oparg;
				int path_len = strlen(s);
				if(path_len == 0 || s == NULL){
					fprintf(stderr,"Invalid file path.\n");
					ret = EXIT_FAILURE;
					break;
				}
				prog_args->file_path = (char*)malloc(path_len+1);
				if(prog_args->file_path == NULL){
					fprintf(stderr,"No memory for file name\n");
					return EXIT_FAILURE;
				}
				strcpy(prog_args->file_path,s);
				break;
			}
			default:{
				ret = EXIT_FAILURE;
				break;
			}
		}

	}

	if(prog_args->b < (prog_args->t*prog_args->c+1) ){
		fprintf(stderr,"Buffer size is small, given %d\n",prog_args->b);
		ret = EXIT_FAILURE;
	}

	if(ret == EXIT_FAILURE){
		free(prog_args->file_path); //Even though it could be NULL we can call free
		print_program_usage();
		exit(EXIT_FAILURE);
	}

	return EXIT_SUCCESS;
}
void sig_handler(int sig){
	//Suppress the warning
	(void)sig;

	if(in_file_desc != -1){
		close(in_file_desc);
	}
	munmap(cit_ipc_data->citizens, cit_ipc_data->number_of_citizens*sizeof(citizen_struct));
	munmap(cit_ipc_data,sizeof(cit_ipc_data_struct));
	munmap(buf_ipc_data->buffer, buf_ipc_data->buffer_size*sizeof(char));
	munmap(buf_ipc_data,sizeof(buf_ipc_data_struct));
	munmap(nurses_shared_counter,sizeof(int));
	munmap(vac_ipc_data,sizeof(vac_ipc_data_struct));
	

	exit(EXIT_FAILURE);
}
