/*
	Autor = Ozan GEÇKIN
	1801042103
 */
#include "helper.h"

void nurse(const int no, const int filed, const int number_of_shots, const int number_of_citizens, int * nurses_shared_counter){
	
	char c;
	int has_read = 0;
	int finish_process = 0;
	
	while(1){	
		if(has_read == 0){
			sem_wait(filemutex);
			ssize_t r = read(filed, &c, 1);
			if(r == 0){
				sem_post(filemutex);
				break;
			}
			else{
				has_read = 1;
				fflush(stdout);
			}
			sem_post(filemutex);
		}

		//order is important
		sem_wait(empty);
		sem_wait(bufmutex); //join critical region
		if(has_read == 1 && (c == '1' || c == '2')){
			/*
				We have one buffer! Since we know the number of vaccines beforehand
				First part of the buffer will contain only vaccine1 
				Second half will contain the vaccine2 
				Dividing into two sections yields more easy set/get operations from the array
			*/
			if(c == '1'){
				buf_ipc_data->buffer[0 + buf_ipc_data->v1_ctr] = c;
				buf_ipc_data->v1_ctr++;
			}
			else if(c == '2'){
				const int offset = number_of_shots * number_of_citizens;
				buf_ipc_data->buffer[offset + buf_ipc_data->v2_ctr] = c;
				buf_ipc_data->v2_ctr++;
			}

			printf("Nurse %d (pid=%d) has brought vaccine %c: the clinic has %d vaccine1 and %d vaccine2.\n",no,getpid(),c,buf_ipc_data->v1_ctr,buf_ipc_data->v2_ctr);
			*nurses_shared_counter = *nurses_shared_counter - 1;
			//Check the last vaccine

			 //Even if the file has more data than expected, the process will kill itself because we have carried the expected amount of vaccine to the buffer
			finish_process = (*nurses_shared_counter <= 0);
			if(*nurses_shared_counter == 0){
				//Just entered one time to print it!
				printf("Nurses have carried all vaccines to the buffer, terminating.\n");
			}

			has_read = 0; //Now we used what we read, clear it to read again
			//order is important
			sem_post(bufmutex);
			sem_post(full);
		}

		if(finish_process == 1){
			break;
		}
	}	

}