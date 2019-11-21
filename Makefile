include makefile.common

BIN			= cachedfs
BUILD_DIR	= build

SRCS	= main.c cachedfs.c

OBJS	= $(SRCS:.c=.o)

all		: ../$(BUILD_DIR)/$(BIN)

debug	:	BIN=cachedfs_debug
debug	:	ACTNAME=debug
debug	:	CFLAGS+=$(DEBUG_FLAGS)
debug	: 	CHKBLD $(OBJS) HANDLERS ../$(BUILD_DIR)/$(BIN)

../$(BUILD_DIR)/$(BIN): CHKBLD $(OBJS) HANDLERS
	echo "$(ACTNAME)" > $(BTYPE_FILE)
	$(CC) $(CFLAGS) $(OBJS) handlers/*.o $(DEPENDS) -o ../$(BUILD_DIR)/$(BIN)

install	:	BIN=cachedfs
install	:	CHKBLD all
	$(MAKE) -C .. install

HANDLERS:
	$(MAKE) -C handlers $(ACTNAME)

clean: 
	$(RM) *.o
	$(RM) $(BTYPE_FILE)
	$(MAKE) -C handlers clean

