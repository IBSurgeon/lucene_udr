# ---------------------------------------------------------------------
# Firebird UDR Makefile for Linux GCC
# ---------------------------------------------------------------------
# File name   : makefile
# Description : UDR for Lucene Full-Search
# Author      : Simonov Denis
# Copyright   : (C) 2022 IBase
# All Rights Reserved.
# ---------------------------------------------------------------------


# ---------------------------------------------------------------------
# FIREBIRD Installation Directory
# ---------------------------------------------------------------------
FIREBIRD=		/opt/firebird


# ---------------------------------------------------------------------
# GCC Installation Directory
# ---------------------------------------------------------------------
GCCDIR=		

# ---------------------------------------------------------------------
# General Compiler and linker Defines for Linux
# ---------------------------------------------------------------------
ARCH	:= $(shell arch)	
CC=		    gcc
CXX=		g++
LINK=		g++
LIB_LINK       = cc

ifeq ($(ARCH),x86_64)
CFLAGS= -pthread -ggdb -O3 -DLINUX -DAMD64 -fno-omit-frame-pointer -fno-builtin -pipe -MMD -fPIC -c -I. -I./include -DFIREBIRD -DHAVE_CONFIG_H -std=c++1z
else
CFLAGS=	-pthread -ggdb -O3 -fno-omit-frame-pointer -fno-builtin -pipe -MMD -fPIC -c -I. -I./include -DFIREBIRD -DHAVE_CONFIG_H -std=c++1z
endif
LINK_FLAGS=	-L. ./defs/udr_plugin.def -L$(FIREBIRD)/lib
LIB_LINK_FLAGS=	-shared -Wl,-s -Wl,-x
LIB_LINK_SONAME:= -Wl,-soname,
LIB_LINK_RPATH:= -Wl,-rpath,
CP=		cp
RM=		rm -f
TARGETS= luceneudr
MKD = mkdir -p
CHMOD = chmod a+rwx


# ---------------------------------------------------------------------
# Generic Compilation Rules 
# ---------------------------------------------------------------------
lucene_udr_objects = FTS.o FTSIndex.o FTSLog.o Relations.o

.SUFFIXES: .o .cpp .c

.cpp.o:
	$(CC) $< $(CFLAGS) $<

.c.o:
	$(CC) $< $(CFLAGS) $<

all:	$(TARGETS)

luceneudr: $(lucene_udr_objects) 
	$(LIB_LINK) $(LIB_LINK_FLAGS) $(LIB_LINK_SONAME)libluceneudr.so $(LIB_LINK_RPATH)$(FIREBIRD)/lib -o ./Linux_x64/lib$@.so $(lucene_udr_objects) -pthread -lpthread -lib_util -lfbclient -lstdc++

	@echo ------------------------------------------------------
	@echo You need to copy libluceneudr.so to the firebird plugins/udr directory
	@echo ------------------------------------------------------

install:
	$(CP) ./Linux_x64/libluceneudr.so $(FIREBIRD)/plugins/udr/

mdiruu:
	$(MKD) $(UUIDDIR)

cmuudir:
	$(CHMOD) $(UUIDDIR)

clean:
	$(RM) *.o core a.out

clobber: clean
	$(RM) ./Linux_x64/libluceneudr.so libluceneudr

