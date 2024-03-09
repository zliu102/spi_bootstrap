# contrib/reservoir_sampling/Makefile

MODULE_big = spi_bootstrap
OBJS = spi_bootstrap.o $(WIN32RES)
EXTENSION = spi_bootstrap
DATA = spi_bootstrap--1.0.sql
PGFILEDESC = "spi_bootstrap - binary search for integer arrays"
PG_CFLAGS += -ggdb -O0

REGRESS = spi_bootstrap

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/spi_bootstrap
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
