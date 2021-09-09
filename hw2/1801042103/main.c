/* Author : Ozan GECKIN
            1801042103
*/
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <signal.h>
#include <stdlib.h>
#include <stdbool.h>

//The buffer I prepared for you to keep the lines.
#define BUFFER_MAX_SIZE 127
bool ctrl_c = false;
//I have defined the functions I use
int readLine(char *buf,int size,char *fp,off_t *offset);
void parser(char line[BUFFER_MAX_SIZE],int pid);
void lagrangeCalculater(float x[8], float y[8],int pid);
void SignalHandler (int signal);

//The main function takes the filename and checks it.
//Creates a child process with fork.
//Calls the readLine function and reads from the file.
//It sends the line it reads to the child process id parser function.
int main(int argc, char **argv){
    
    // Setting up the signal handler to answer to ctrl+c
    struct sigaction act;
    act.sa_handler = SignalHandler;
    sigemptyset (&act.sa_mask);
    act.sa_flags = 0;
    sigaction(SIGINT, &act, NULL);

    char line[BUFFER_MAX_SIZE] = {0};
    int pidArray[100],len = 0,i=0;
    off_t offset = 0;
    pid_t pid;
    char fileName[20];

    if(argc == 2){ 
        strcpy(fileName,argv[1]);
    }else{
        fprintf(stderr,"Wrong argument\n");
    }

    for(int i=0;i<8;i++){
        pid=fork();
        if(pid ==0){
            exit(1);
        }else if(pid>0){
            pidArray[i]=pid;
        }else{
            fprintf(stderr,"No fork\n");
        }
    }
    i=0;
    while((len = readLine (line, BUFFER_MAX_SIZE, fileName, &offset)) != -1){
        parser(line,pidArray[i]);
        i++;       
    }
    
}
//It performs file operations with the parameters it receives and returns how many characters it reads.
int readLine(char *buf,int size,char *fp,off_t *offset){
    int fd=open(fp,O_RDWR);
    int check=0;
    int index=0;
    char *p=NULL;

    if(fd==-1){
        fprintf(stderr,"Error:file open failed '%s'.\n",fp);
        return -1;
    }
    if((check = lseek(fd, *offset, SEEK_SET))!=-1){
        check=read(fd,buf,size);
    }
    
    close(fd);
    if(check == -1){
        fprintf(stderr,"Error: read failure in '%s'",fp);
        return check;
    }
    if(check==0){
        return -1;
    }
     p = buf; 
    while (index < check && *p != '\n') p++, index++;
    *p = 0;
    if (index == check) {
        *offset += check;
        return check < size ? check : 0;
    }
    *offset += index + 1;

    return index;    
}
//parses the received line and sends it to langrange computation.
void parser(char line[BUFFER_MAX_SIZE],int pid){  
    float x[8],y[8],temp[5000];
    char* token;
    int counter=0,xCounter=0,yCounter=0,flag=0,i=0;
    
    token = strtok(line,",");
    while(token !=NULL ){
        temp[counter]=atof(token);
        token= strtok(NULL,",");
        counter++;
    }
    int tt=0;
    i=0;
    while(tt<16) {
        if(flag==0){
            x[xCounter]=temp[i];
          printf("x[%d] = %.1f ",xCounter,x[xCounter]);
            flag=1;
            xCounter++;
        }else{
            y[yCounter]=temp[i];
          printf("y[%d] = %.1f ",yCounter ,y[yCounter]);
            flag=0;
            yCounter++;
        }
        i++;
        tt++;
    }
    lagrangeCalculater(x,y,pid);
}
//calculating lagrangeCalculater
void lagrangeCalculater(float x[8], float y[8],int pid){
    float upper=0,lower=0,sum=0;
    for(int q=1;q<=6;q++){
		lower=1;
		for(int j=1;j<=6;j++){
		    if(q!=j){
			    	lower = lower * (upper - x[j])/(x[q] - x[j]);
			}
		}
		sum = sum + lower * y[q];
	}
    printf("\nPid id: %d -->Polinominal : %.1f\n",pid,sum);
}
// sets global var to true in order to know if ctrl
// was called.
void SignalHandler (int signal)
{
    ctrl_c = true;
}