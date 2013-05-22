const char* svn_version() { const char* SVN_Version = "13572"; return SVN_Version; }
const char* build_date() { return __DATE__; }
const char* build_time() { return __TIME__; }
const char* build_flags() { return "-g -O2 -D__UNIT_TEST__ -D__VERSION_ID__=updateserver 1.0.0.0 -D_BTREE_ENGINE_"; }
