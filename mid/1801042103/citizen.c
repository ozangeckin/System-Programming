/*
	Autor = Ozan GEÇKIN
	1801042103
 */
#include "helper.h"
#include <signal.h>
#include <stdlib.h>


int pid_comparator(const void * a, const void * b) {
	//Since pid values are integers cast it to the integers
	//the citizen process with the smallest pid will be considered the oldest, so put it first
	citizen_struct * csa = (citizen_struct*)a;
	citizen_struct * csb = (citizen_struct*)b;
   return ( csa->pid - csb->pid );
}

void order_citizens_by_age(){
	//Must be called after entring the critical region by citmutex!
	//And make sure buffer len is equal to number of citizens
	qsort(cit_ipc_data->citizens, cit_ipc_data->number_of_citizens, sizeof(citizen_struct), pid_comparator);

}

citizen_struct * find_citizen_pointer_by_pid(int pid){
	//Search in array and return its pointer, call it after entring the critical region
	int i;
	for( i = 0; i<cit_ipc_data->citizens_buffer_len; ++i){
		if(cit_ipc_data->citizens[i].pid == pid){
			return cit_ipc_data->citizens+i;
		}
	}
	return NULL;
}

void citizen(const int no, const int t){
	

	// Now let's block SIGUSR1 which means we get invited to the clinic
	sigset_t sigset;
	sigemptyset(&sigset);
	sigaddset(&sigset, SIGUSR1);
	sigprocmask(SIG_BLOCK, &sigset, NULL);

	sem_wait(citmutex); //if we have the citmutex we can enter the critical region
	
	cit_ipc_data->citizens[cit_ipc_data->citizens_buffer_len].pid = getpid();
	cit_ipc_data->citizens[cit_ipc_data->citizens_buffer_len].shot_number = 0;
	cit_ipc_data->citizens_buffer_len++;
	if(cit_ipc_data->citizens_buffer_len == cit_ipc_data->number_of_citizens){
		//The last process entered here are responsible to sort citizen array
		order_citizens_by_age();
	}

	sem_post(citmutex);

	while(1){
		int sig;
		int result = sigwait(&sigset, &sig); //Wait for the vaccinator to call
		if(result == 0){
			if(sig == SIGUSR1){
				
				sem_wait(citmutex); //if we have the citmutex we can enter the critical region
				
				//Hold locally which part of the array this citizen has its pid | shot number etc.
				citizen_struct * itself = find_citizen_pointer_by_pid(getpid());

				printf("Citizen %d (pid=%d) is vaccinated for %d times: the clinic has %d vaccine1 and %d vaccine2.\n",no,getpid(),itself->shot_number,buf_ipc_data->v1_ctr,buf_ipc_data->v2_ctr);
				if(itself->shot_number == t){
					int remaining_ct = cit_ipc_data->number_of_citizens-cit_ipc_data->vaccinated_citizens;
					printf("Citizen %d (pid=%d) is leaving. Remaining %d citizens.\n",no,getpid(),remaining_ct);
					sem_post(citmutex);
					break;	
				}
				sem_post(citmutex);
			}

		}
		else{
			fprintf(stderr,"Got the wrong signal\n");
			return;
		}
	}
	
}
