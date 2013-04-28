
LIBPROFILER_HELPER_VERSION=1:0:0

lib_LTLIBRARIES = libprofiler_helper.la
libprofiler_helper_la_SOURCES = profiler_helper.c

libprofiler_helper_la_CPPFLAGS = -g -O2 -fPIC

libprofiler_helper_la_LDFLAGS = \
                      -version-info $(LIBPROFILER_HELPER_VERSION) \
                      ./libprofiler.so ./libunwind.so \
                      -Wl,-e,so_main

AM_LDFLAGS = -lpthread -ldl

EXTRA_DIST = \
			 profiler.h \
			 libprofiler.so \
			 libprofiler.so.0 \
			 libprofiler.so.0.3.0 \
			 libunwind.so \
			 libunwind.so.8 \
			 libunwind.so.8.0.1
