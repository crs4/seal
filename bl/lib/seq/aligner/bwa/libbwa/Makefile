CC=			gcc
CXX=		g++
CFLAGS=		-g -Wall -O2 -m64
CFLAGS_LIB=	-fPIC -Wall -O3 -m64
CXXFLAGS=	$(CFLAGS)
CXXFLAGS_LIB=	$(CFLAGS_LIB)

BWA_LIBRARY_DIR ?= ..

# CRS4_DEBUG_RANDOMNESS eliminates randomness in N base replacement
# and hit selection: this allows to perform comparisons between the
# python wrapper and the original code.

DFLAGS=		-DHAVE_PTHREAD #-DCRS4_DEBUG_RANDOMNESS #-D_FILE_OFFSET_BITS=64
DFLAGS_LIB=	-DHAVE_PTHREAD -DBWT_ENABLE_MMAP -DBWT_EXPORT_LIBRARY_FUNCTIONS #-DCRS4_DEBUG_RANDOMNESS #-D_FILE_OFFSET_BITS=64

OBJS=		utils.o bwt.o bwtio.o bwtaln.o bwtgap.o is.o \
			bntseq.o bwtmisc.o bwtindex.o stdaln.o simple_dp.o \
			bwaseqio.o bwase.o bwape.o kstring.o cs2nt.o \
			bwtsw2_core.o bwtsw2_main.o bwtsw2_aux.o bwt_lite.o \
			bwtsw2_chain.o bamlite.o
OBJS_LIB=	bwtaln.ol bwtgap.ol bwt.ol bwape.ol bwase.ol stdaln.ol \
			bwtio.ol bwaseqio.ol bntseq.ol cs2nt.ol kstring.ol \
			utils.ol
PROG=		bwa
INCLUDES=	
LIBS=		-lm -lz -lpthread -Lbwt_gen -lbwtgen
LIBS_LIB=	-lz
SUBDIRS=	. bwt_gen
BWA_LIBRARY_NAME=$(BWA_LIBRARY_DIR)/libbwa

.SUFFIXES:.c .o .cc .ol

.c.o:
		$(CC) -c $(CFLAGS) $(DFLAGS) $(INCLUDES) $< -o $@
.cc.o:
		$(CXX) -c $(CXXFLAGS) $(DFLAGS) $(INCLUDES) $< -o $@
.c.ol:
		$(CC) -c $(CFLAGS_LIB) $(DFLAGS_LIB) $(INCLUDES) $< -o $@
.cc.ol:
		$(CXX) -c $(CXXFLAGS_LIB) $(DFLAGS_LIB) $(INCLUDES) $< -o $@

all:$(PROG)

lib-recur all-recur clean-recur cleanlocal-recur install-recur:
		@target=`echo $@ | sed s/-recur//`; \
		wdir=`pwd`; \
		list='$(SUBDIRS)'; for subdir in $$list; do \
			cd $$subdir; \
			$(MAKE) CC="$(CC)" CXX="$(CXX)" DFLAGS="$(DFLAGS)" CFLAGS="$(CFLAGS)" \
				INCLUDES="$(INCLUDES)" $$target || exit 1; \
			cd $$wdir; \
		done;

lib:
libbwa: ${BWA_LIBRARY_NAME}.so

${BWA_LIBRARY_NAME}.so:  ${OBJS_LIB}
	$(CC) -shared  $^ ${LIBS_LIB} -o $@

bwa:lib-recur $(OBJS) main.o
		$(CC) $(CFLAGS) $(DFLAGS) $(OBJS) main.o -o $@ $(LIBS)

bwt.o:bwt.h
bwtio.o:bwt.h
bwtaln.o:bwt.h bwtaln.h kseq.h
bwt1away.o:bwt.h bwtaln.h
bwt2fmv.o:bwt.h
bntseq.o:bntseq.h
bwtgap.o:bwtgap.h bwtaln.h bwt.h

bwtsw2_core.o:bwtsw2.h bwt.h bwt_lite.h stdaln.h
bwtsw2_aux.o:bwtsw2.h bwt.h bwt_lite.h stdaln.h
bwtsw2_main.o:bwtsw2.h

# FOR LIB
bwt.ol:bwt.h
bwtio.ol:bwt.h
bwtaln.ol:bwt.h bwtaln.h kseq.h
bntseq.ol:bntseq.h
bwtgap.ol:bwtgap.h bwtaln.h bwt.h


cleanlocal:
		rm -f gmon.out *.o a.out $(PROG) *~ *.a

cleanlib:
		rm -f *.ol *~

clean:cleanlocal-recur
