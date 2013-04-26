EXTRA_DIST=build.sh rpm/oceanbase.spec svn_dist_version rpm/oceanbase_test.spec script/deploy
sysconf_DATA=script/deploy/oceanbase.conf.template
bin_SCRIPTS=script/deploy/oceanbase.pl

if WITH_TEST_CASE
TEST_DIR:=tests
endif
SUBDIRS=src $(TEST_DIR) tools
DIST_SUBDIRS=src tests tools

utest: check
.PHONY: utest

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk
