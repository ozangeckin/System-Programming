_SRC = main.c nurse.c vaccinator.c citizen.c
OBJ = ${_SRC:.c=.o}
INC = .


INC_FLAGS := $(addprefix -I,$(INC))

CC=gcc

CFLAGS = -c -Wall -Wextra
LDFLAGS = -lpthread -lrt

%.o:%.c
	@$(CC) -c -o $@ $< $(CFLAGS)

OUT = program

all: program
$(OUT): $(OBJ)
	@$(CC) -o $@ $^ $(LDFLAGS) 

clean:
	@rm -f $(OBJ)
