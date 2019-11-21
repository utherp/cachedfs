include ../makefile.common

SRCS	= file.c dir.c attr.c link.c

all: $(OBJS)

debug: CFLAGS+=$(DEBUG_FLAGS)
debug: $(OBJS)

clean: 
	$(RM) *.o

