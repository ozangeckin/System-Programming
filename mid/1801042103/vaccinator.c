/*
	Autor = Ozan GEÇKIN
	1801042103
 */
#include "helper.h"
#include <signal.h>

void vaccinator(const int no, const int number_of_shots, const int number_of_citizens){
	
	int local_vaccination_ctr = 0;
	int has_two_vaccines = 0;
	int i;
	while(1){

		if(has_two_vaccines == 0){
			sem_wait(vacmutex);
			if(vac_ipc_data->kill_process == 1){
				/*
					As explained below, we post full semaphore to make sure there is no vaccinator process
					sitting idle to wait data even though there is no data
					Our if-else sections also protects the data so there is no extra reading because of this post
				*/
				sem_post(full);
				sem_post(vacmutex);
				break;	
			}
			sem_post(vacmutex);

			//order is important
			sem_wait(full); //if zero we wait here otherwise enters
			sem_wait(bufmutex); //if we have the bufmutex we can enter the critical region

			if(buf_ipc_data->v1_ctr > 0 && buf_ipc_data->v2_ctr > 0){

				const int v2_array_offset = number_of_shots * number_of_citizens;

				char v1 = buf_ipc_data->buffer[0 + buf_ipc_data->v1_ctr - 1];
				char v2 = buf_ipc_data->buffer[v2_array_offset + buf_ipc_data->v2_ctr -1];

				if(v1 != '1' || v2 != '2'){
					fprintf(stderr,"Nurses put wrong vaccines\n");
				}
				
				--buf_ipc_data->v1_ctr;
				--buf_ipc_data->v2_ctr;
				
				sem_wait(vacmutex);
				vac_ipc_data->no_vaccination++;
				if(vac_ipc_data->no_vaccination == (number_of_citizens*number_of_shots)){
					vac_ipc_data->kill_process = 1;
			
					sem_post(full);
					printf("------> Giving the kill signal to vaccinator process i am %d\n",no);
				}
				sem_post(vacmutex);

				has_two_vaccines = 1;

				printf("Vaccinator %d (pid=%d) put vaccines in his bag\n",no,getpid());
				//order is important
				sem_post(bufmutex);
				sem_post(empty);
			}
			else{
				sem_post(bufmutex);
			}
			
		}
		
		//Now vaccination comes
		if(has_two_vaccines == 1){
			sem_wait(citmutex);
			for( i = 0; i<cit_ipc_data->citizens_buffer_len; ++i){
				citizen_struct * cs = cit_ipc_data->citizens+i; //get the citizen pointer
				if(cs->shot_number != cit_ipc_data->number_of_shots){
					//invite the citizen by sending the signal
					printf("Vaccinator %d (pid=%d) is inviting citizen pid=%d to the clinic.\n",no,getpid(),cs->pid);
					kill(cs->pid,SIGUSR1);
					cs->shot_number++;
					if(cs->shot_number == cit_ipc_data->number_of_shots){
						cit_ipc_data->vaccinated_citizens++;
					}
					local_vaccination_ctr += 2; //Since we have vaccinated two times with v1 and v2
					has_two_vaccines = 0;
					break;
				}
			}

			sem_post(citmutex);
		}

	}
	printf("Vaccinator %d vaccinated %d times\n",no,local_vaccination_ctr);
}