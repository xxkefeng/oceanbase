/* A Bison parser, made by GNU Bison 2.5.  */

/* Bison implementation for Yacc-like parsers in C
   
      Copyright (C) 1984, 1989-1990, 2000-2011 Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "2.5"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 1

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1

/* Using locations.  */
#define YYLSP_NEEDED 1



/* Copy the first part of user declarations.  */


#include <stdint.h>
#include "parse_node.h"
#include "parse_malloc.h"
#include "ob_non_reserved_keywords.h"
#include "common/ob_privilege_type.h"
#define YYDEBUG 1



/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* Enabling the token table.  */
#ifndef YYTOKEN_TABLE
# define YYTOKEN_TABLE 0
#endif


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     NAME = 258,
     STRING = 259,
     INTNUM = 260,
     DATE_VALUE = 261,
     HINT_VALUE = 262,
     BOOL = 263,
     APPROXNUM = 264,
     NULLX = 265,
     UNKNOWN = 266,
     QUESTIONMARK = 267,
     SYSTEM_VARIABLE = 268,
     TEMP_VARIABLE = 269,
     EXCEPT = 270,
     UNION = 271,
     INTERSECT = 272,
     OR = 273,
     AND = 274,
     NOT = 275,
     COMP_NE = 276,
     COMP_GE = 277,
     COMP_GT = 278,
     COMP_EQ = 279,
     COMP_LT = 280,
     COMP_LE = 281,
     CNNOP = 282,
     LIKE = 283,
     BETWEEN = 284,
     IN = 285,
     IS = 286,
     MOD = 287,
     UMINUS = 288,
     ADD = 289,
     ANY = 290,
     ALL = 291,
     ALTER = 292,
     AS = 293,
     ASC = 294,
     BEGI = 295,
     BIGINT = 296,
     BINARY = 297,
     BOOLEAN = 298,
     BOTH = 299,
     BY = 300,
     CASCADE = 301,
     CASE = 302,
     CHARACTER = 303,
     CLUSTER = 304,
     COMMENT = 305,
     COMMIT = 306,
     CONSISTENT = 307,
     COLUMN = 308,
     COLUMNS = 309,
     CREATE = 310,
     CREATETIME = 311,
     CURRENT_USER = 312,
     DATE = 313,
     DATETIME = 314,
     DEALLOCATE = 315,
     DECIMAL = 316,
     DEFAULT = 317,
     DELETE = 318,
     DESC = 319,
     DESCRIBE = 320,
     DISTINCT = 321,
     DOUBLE = 322,
     DROP = 323,
     DUAL = 324,
     ELSE = 325,
     END = 326,
     END_P = 327,
     ERROR = 328,
     EXECUTE = 329,
     EXISTS = 330,
     EXPLAIN = 331,
     FLOAT = 332,
     FOR = 333,
     FROM = 334,
     FULL = 335,
     GLOBAL = 336,
     GLOBAL_ALIAS = 337,
     GRANT = 338,
     GROUP = 339,
     HAVING = 340,
     HINT_BEGIN = 341,
     HINT_END = 342,
     IDENTIFIED = 343,
     IF = 344,
     INDEX = 345,
     INNER = 346,
     INTEGER = 347,
     INSERT = 348,
     INTO = 349,
     JOIN = 350,
     KEY = 351,
     LEADING = 352,
     LEFT = 353,
     LIMIT = 354,
     LOCAL = 355,
     LOCKED = 356,
     MEDIUMINT = 357,
     MEMORY = 358,
     MODIFYTIME = 359,
     NUMERIC = 360,
     OFFSET = 361,
     ON = 362,
     ORDER = 363,
     OPTION = 364,
     OUTER = 365,
     PARAMETERS = 366,
     PASSWORD = 367,
     PRECISION = 368,
     PREPARE = 369,
     PRIMARY = 370,
     READ_CONSISTENCY = 371,
     READ_STATIC = 372,
     REAL = 373,
     RENAME = 374,
     REPLACE = 375,
     RESTRICT = 376,
     PRIVILEGES = 377,
     REVOKE = 378,
     RIGHT = 379,
     ROLLBACK = 380,
     SCHEMA = 381,
     SCOPE = 382,
     SELECT = 383,
     SESSION = 384,
     SESSION_ALIAS = 385,
     SET = 386,
     SHOW = 387,
     SMALLINT = 388,
     SNAPSHOT = 389,
     SPFILE = 390,
     START = 391,
     STATIC = 392,
     STORING = 393,
     STRONG = 394,
     SYSTEM = 395,
     TABLE = 396,
     TABLES = 397,
     THEN = 398,
     TIME = 399,
     TIMESTAMP = 400,
     TINYINT = 401,
     TRAILING = 402,
     TRANSACTION = 403,
     TO = 404,
     UNIQUE = 405,
     UPDATE = 406,
     USER = 407,
     USING = 408,
     VALUES = 409,
     VARCHAR = 410,
     VARBINARY = 411,
     WEAK = 412,
     WHERE = 413,
     WHEN = 414,
     WITH = 415,
     WORK = 416,
     AUTO_INCREMENT = 417,
     CHARSET = 418,
     CHUNKSERVER = 419,
     COMPRESS_METHOD = 420,
     CONSISTENT_MODE = 421,
     EXPIRE_INFO = 422,
     GRANTS = 423,
     MERGESERVER = 424,
     REPLICA_NUM = 425,
     ROOTSERVER = 426,
     SERVER = 427,
     SERVER_IP = 428,
     SERVER_PORT = 429,
     SERVER_TYPE = 430,
     STATUS = 431,
     TABLET_BLOCK_SIZE = 432,
     TABLET_MAX_SIZE = 433,
     UNLOCKED = 434,
     UPDATESERVER = 435,
     USE_BLOOM_FILTER = 436,
     VARIABLES = 437,
     VERBOSE = 438,
     WARNINGS = 439
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{


  struct _ParseNode *node;
  const struct _NonReservedKeyword *non_reserved_keyword;
  int    ival;



} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


/* Copy the second part of user declarations.  */


#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>

#include "sql_parser.lex.h"

#define YYLEX_PARAM result->yyscan_info_

extern void yyerror(YYLTYPE* yylloc, ParseResult* p, char* s,...);

extern ParseNode* merge_tree(void *malloc_pool, ObItemType node_tag, ParseNode* source_tree);

extern ParseNode* new_terminal_node(void *malloc_pool, ObItemType type);

extern ParseNode* new_non_terminal_node(void *malloc_pool, ObItemType node_tag, int num, ...);

extern char* copy_expr_string(ParseResult* p, int expr_start, int expr_end);

#define malloc_terminal_node(node, malloc_pool, type) \
do \
{ \
  if ((node = new_terminal_node(malloc_pool, type)) == NULL) \
  { \
    yyerror(NULL, result, "No more space for malloc"); \
    YYABORT; \
  } \
} while(0)

#define malloc_non_terminal_node(node, malloc_pool, node_tag, ...) \
do \
{ \
  if ((node = new_non_terminal_node(malloc_pool, node_tag, ##__VA_ARGS__)) == NULL) \
  { \
    yyerror(NULL, result, "No more space for malloc"); \
    YYABORT; \
  } \
} while(0)

#define merge_nodes(node, malloc_pool, node_tag, source_tree) \
do \
{ \
  if (source_tree == NULL) \
  { \
    node = NULL; \
  } \
  else if ((node = merge_tree(malloc_pool, node_tag, source_tree)) == NULL) \
  { \
    yyerror(NULL, result, "No more space for merging nodes"); \
    YYABORT; \
  } \
} while (0)

#define dup_expr_string(str_ptr, result, expr_start, expr_end) \
  do \
  { \
    str_ptr = NULL; \
    if (expr_end >= expr_start \
      && (str_ptr = copy_expr_string(result, expr_start, expr_end)) == NULL) \
    { \
      yyerror(NULL, result, "No more space for copying expression string"); \
      YYABORT; \
    } \
  } while (0)




#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#elif (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
typedef signed char yytype_int8;
#else
typedef short int yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(e) ((void) (e))
#else
# define YYUSE(e) /* empty */
#endif

/* Identity function, used to suppress warnings about constant conditions.  */
#ifndef lint
# define YYID(n) (n)
#else
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static int
YYID (int yyi)
#else
static int
YYID (yyi)
    int yyi;
#endif
{
  return yyi;
}
#endif

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's `empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (YYID (0))
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
	     && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
	 || (defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL \
	     && defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE) + sizeof (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)				\
    do									\
      {									\
	YYSIZE_T yynewbytes;						\
	YYCOPY (&yyptr->Stack_alloc, Stack, yysize);			\
	Stack = &yyptr->Stack_alloc;					\
	yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
	yyptr += yynewbytes / sizeof (*yyptr);				\
      }									\
    while (YYID (0))

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  YYSIZE_T yyi;				\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];		\
	}					\
      while (YYID (0))
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  160
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   2560

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  196
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  154
/* YYNRULES -- Number of rules.  */
#define YYNRULES  476
/* YYNRULES -- Number of states.  */
#define YYNSTATES  847

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   439

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,    36,     2,     2,
      40,    41,    34,    32,   195,    33,    42,    35,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   194,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,    38,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    37,    39,    43,
      44,    45,    46,    47,    48,    49,    50,    51,    52,    53,
      54,    55,    56,    57,    58,    59,    60,    61,    62,    63,
      64,    65,    66,    67,    68,    69,    70,    71,    72,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,    98,    99,   100,   101,   102,   103,
     104,   105,   106,   107,   108,   109,   110,   111,   112,   113,
     114,   115,   116,   117,   118,   119,   120,   121,   122,   123,
     124,   125,   126,   127,   128,   129,   130,   131,   132,   133,
     134,   135,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,   163,
     164,   165,   166,   167,   168,   169,   170,   171,   172,   173,
     174,   175,   176,   177,   178,   179,   180,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     6,    10,    12,    14,    16,    18,    20,
      22,    24,    26,    28,    30,    32,    34,    36,    38,    40,
      42,    44,    46,    48,    50,    52,    54,    56,    58,    60,
      62,    64,    65,    67,    71,    73,    77,    81,    83,    85,
      87,    89,    91,    93,    95,    97,    99,   103,   105,   107,
     111,   117,   119,   121,   123,   126,   128,   131,   134,   138,
     142,   146,   150,   154,   158,   162,   164,   167,   170,   174,
     178,   182,   186,   190,   194,   198,   202,   206,   210,   214,
     218,   222,   226,   231,   235,   239,   242,   246,   251,   255,
     260,   264,   269,   275,   282,   286,   291,   295,   297,   301,
     307,   309,   310,   312,   315,   320,   323,   324,   329,   335,
     340,   347,   352,   356,   358,   360,   365,   371,   373,   377,
     381,   390,   394,   395,   397,   401,   403,   409,   413,   415,
     417,   419,   421,   423,   426,   429,   431,   434,   436,   439,
     442,   444,   447,   450,   453,   456,   458,   460,   462,   465,
     471,   475,   476,   480,   481,   483,   484,   488,   489,   493,
     494,   497,   498,   501,   503,   506,   508,   511,   513,   517,
     518,   522,   526,   530,   534,   538,   542,   546,   551,   557,
     559,   560,   562,   563,   575,   577,   578,   580,   582,   586,
     589,   594,   595,   600,   601,   604,   606,   610,   614,   621,
     626,   634,   636,   638,   642,   643,   645,   649,   653,   659,
     661,   665,   667,   669,   673,   677,   679,   682,   686,   691,
     697,   706,   708,   710,   720,   725,   730,   735,   736,   739,
     743,   748,   753,   756,   759,   764,   765,   769,   771,   775,
     776,   778,   783,   786,   788,   790,   792,   794,   795,   797,
     798,   801,   805,   810,   815,   820,   824,   828,   832,   833,
     837,   839,   840,   844,   846,   850,   853,   854,   856,   858,
     859,   862,   863,   865,   867,   869,   872,   876,   878,   880,
     884,   886,   890,   892,   896,   899,   903,   906,   908,   914,
     916,   920,   927,   933,   936,   939,   942,   944,   946,   947,
     951,   953,   955,   957,   959,   961,   962,   967,   973,   979,
     984,   989,   994,   997,  1002,  1006,  1010,  1014,  1018,  1022,
    1026,  1032,  1037,  1040,  1041,  1043,  1046,  1051,  1053,  1055,
    1056,  1057,  1060,  1063,  1064,  1066,  1070,  1072,  1076,  1081,
    1083,  1085,  1089,  1091,  1095,  1101,  1108,  1111,  1112,  1116,
    1120,  1122,  1126,  1131,  1133,  1135,  1137,  1138,  1142,  1143,
    1146,  1150,  1153,  1156,  1163,  1165,  1169,  1172,  1174,  1176,
    1179,  1181,  1183,  1186,  1188,  1190,  1192,  1194,  1196,  1197,
    1199,  1201,  1207,  1210,  1211,  1216,  1218,  1220,  1222,  1224,
    1226,  1229,  1231,  1235,  1239,  1243,  1248,  1253,  1259,  1265,
    1269,  1271,  1273,  1275,  1279,  1282,  1283,  1285,  1289,  1293,
    1295,  1297,  1302,  1309,  1311,  1315,  1319,  1324,  1329,  1335,
    1337,  1338,  1340,  1342,  1343,  1347,  1351,  1355,  1358,  1363,
    1365,  1369,  1379,  1382,  1383,  1385,  1386,  1390,  1394,  1398,
    1399,  1401,  1403,  1405,  1407,  1411,  1418,  1419,  1421,  1423,
    1425,  1427,  1429,  1431,  1433,  1435,  1437,  1439,  1441,  1443,
    1445,  1447,  1449,  1451,  1453,  1455,  1457,  1459,  1461,  1463,
    1465,  1467,  1469,  1471,  1473,  1475,  1477
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     197,     0,    -1,   198,    81,    -1,   198,   194,   199,    -1,
     199,    -1,   251,    -1,   245,    -1,   218,    -1,   215,    -1,
     214,    -1,   241,    -1,   283,    -1,   286,    -1,   318,    -1,
     321,    -1,   326,    -1,   331,    -1,   337,    -1,   329,    -1,
     292,    -1,   297,    -1,   299,    -1,   301,    -1,   304,    -1,
     311,    -1,   316,    -1,   308,    -1,   309,    -1,   310,    -1,
     235,    -1,   244,    -1,    -1,   205,    -1,   200,   195,   205,
      -1,   345,    -1,   346,    42,   345,    -1,   346,    42,    34,
      -1,     4,    -1,     6,    -1,     5,    -1,     9,    -1,     8,
      -1,    10,    -1,    12,    -1,    14,    -1,    13,    -1,   139,
      42,   345,    -1,   201,    -1,   202,    -1,    40,   205,    41,
      -1,    40,   200,   195,   205,    41,    -1,   207,    -1,   212,
      -1,   252,    -1,    84,   252,    -1,   203,    -1,    32,   204,
      -1,    33,   204,    -1,   204,    32,   204,    -1,   204,    33,
     204,    -1,   204,    34,   204,    -1,   204,    35,   204,    -1,
     204,    36,   204,    -1,   204,    38,   204,    -1,   204,    37,
     204,    -1,   203,    -1,    32,   205,    -1,    33,   205,    -1,
     205,    32,   205,    -1,   205,    33,   205,    -1,   205,    34,
     205,    -1,   205,    35,   205,    -1,   205,    36,   205,    -1,
     205,    38,   205,    -1,   205,    37,   205,    -1,   205,    26,
     205,    -1,   205,    25,   205,    -1,   205,    24,   205,    -1,
     205,    22,   205,    -1,   205,    23,   205,    -1,   205,    21,
     205,    -1,   205,    28,   205,    -1,   205,    20,    28,   205,
      -1,   205,    19,   205,    -1,   205,    18,   205,    -1,    20,
     205,    -1,   205,    31,    10,    -1,   205,    31,    20,    10,
      -1,   205,    31,     8,    -1,   205,    31,    20,     8,    -1,
     205,    31,    11,    -1,   205,    31,    20,    11,    -1,   205,
      29,   204,    19,   204,    -1,   205,    20,    29,   204,    19,
     204,    -1,   205,    30,   206,    -1,   205,    20,    30,   206,
      -1,   205,    27,   205,    -1,   252,    -1,    40,   200,    41,
      -1,    56,   208,   209,   211,    80,    -1,   205,    -1,    -1,
     210,    -1,   209,   210,    -1,   168,   205,   152,   205,    -1,
      79,   205,    -1,    -1,   347,    40,    34,    41,    -1,   347,
      40,   213,   205,    41,    -1,   347,    40,   200,    41,    -1,
     347,    40,   205,    47,   223,    41,    -1,   347,    40,   266,
      41,    -1,   347,    40,    41,    -1,    45,    -1,    75,    -1,
      72,    88,   279,   257,    -1,   160,   279,   140,   216,   257,
      -1,   217,    -1,   216,   195,   217,    -1,   345,    24,   205,
      -1,    64,   150,   219,   279,    40,   220,    41,   231,    -1,
      98,    20,    84,    -1,    -1,   221,    -1,   220,   195,   221,
      -1,   222,    -1,   124,   105,    40,   248,    41,    -1,   345,
     223,   229,    -1,   155,    -1,   142,    -1,   111,    -1,   101,
      -1,    50,    -1,    70,   224,    -1,   114,   224,    -1,    52,
      -1,    86,   225,    -1,   127,    -1,    76,   226,    -1,   154,
     227,    -1,    68,    -1,    57,   228,    -1,    51,   228,    -1,
     164,   228,    -1,   165,   228,    -1,    65,    -1,   113,    -1,
      67,    -1,   153,   227,    -1,    40,     5,   195,     5,    41,
      -1,    40,     5,    41,    -1,    -1,    40,     5,    41,    -1,
      -1,   122,    -1,    -1,    40,     5,    41,    -1,    -1,    40,
       5,    41,    -1,    -1,   229,   230,    -1,    -1,    20,    10,
      -1,    10,    -1,    71,   202,    -1,   171,    -1,   124,   105,
      -1,   232,    -1,   231,   195,   232,    -1,    -1,   176,   233,
       4,    -1,   187,   233,     5,    -1,   186,   233,     5,    -1,
     179,   233,     5,    -1,   174,   233,     4,    -1,   190,   233,
       8,    -1,   175,   233,   146,    -1,   234,   172,   233,     4,
      -1,   234,    57,   140,   233,     4,    -1,    24,    -1,    -1,
      71,    -1,    -1,    64,   236,    99,   237,   116,   279,    40,
     238,    41,   240,   231,    -1,   159,    -1,    -1,   346,    -1,
     239,    -1,   238,   195,   239,    -1,   345,   272,    -1,   147,
      40,   248,    41,    -1,    -1,    77,   150,   242,   243,    -1,
      -1,    98,    84,    -1,   278,    -1,   243,   195,   278,    -1,
      77,    99,   278,    -1,   246,   103,   279,   247,   163,   249,
      -1,   246,   103,   279,   251,    -1,   246,   103,   279,    40,
     248,    41,   251,    -1,   129,    -1,   102,    -1,    40,   248,
      41,    -1,    -1,   345,    -1,   248,   195,   345,    -1,    40,
     250,    41,    -1,   249,   195,    40,   250,    41,    -1,   205,
      -1,   250,   195,   205,    -1,   253,    -1,   252,    -1,    40,
     253,    41,    -1,    40,   252,    41,    -1,   254,    -1,   256,
     265,    -1,   255,   269,   265,    -1,   255,   268,   258,   265,
      -1,   137,   259,   274,   276,   264,    -1,   137,   259,   274,
     276,    88,    78,   257,   264,    -1,   256,    -1,   252,    -1,
     137,   259,   274,   276,    88,   277,   257,   267,   273,    -1,
     255,    16,   274,   255,    -1,   255,    17,   274,   255,    -1,
     255,    15,   274,   255,    -1,    -1,   167,   205,    -1,   167,
       7,   205,    -1,   108,   263,   115,   263,    -1,   115,   263,
     108,   263,    -1,   108,   263,    -1,   115,   263,    -1,   108,
     263,   195,   263,    -1,    -1,    95,   260,    96,    -1,   261,
      -1,   260,   195,   261,    -1,    -1,   126,    -1,   125,    40,
     262,    41,    -1,    40,    41,    -1,   166,    -1,   148,    -1,
       5,    -1,    12,    -1,    -1,   258,    -1,    -1,    87,   160,
      -1,   205,    88,   205,    -1,    53,   205,    88,   205,    -1,
     106,   205,    88,   205,    -1,   156,   205,    88,   205,    -1,
      53,    88,   205,    -1,   106,    88,   205,    -1,   156,    88,
     205,    -1,    -1,    93,    54,   200,    -1,   269,    -1,    -1,
     117,    54,   270,    -1,   271,    -1,   270,   195,   271,    -1,
     205,   272,    -1,    -1,    48,    -1,    73,    -1,    -1,    94,
     205,    -1,    -1,    45,    -1,    75,    -1,   205,    -1,   205,
     348,    -1,   205,    47,   348,    -1,    34,    -1,   275,    -1,
     276,   195,   275,    -1,   278,    -1,   277,   195,   278,    -1,
     279,    -1,   279,    47,   346,    -1,   279,   346,    -1,   252,
      47,   346,    -1,   252,   346,    -1,   280,    -1,    40,   280,
      41,    47,   346,    -1,   346,    -1,    40,   280,    41,    -1,
     278,   281,   104,   278,   116,   205,    -1,   278,   104,   278,
     116,   205,    -1,    89,   282,    -1,   107,   282,    -1,   133,
     282,    -1,   100,    -1,   119,    -1,    -1,    85,   285,   284,
      -1,   251,    -1,   214,    -1,   245,    -1,   215,    -1,   192,
      -1,    -1,   141,   341,   151,   290,    -1,   141,    63,    88,
     279,   290,    -1,   141,    63,    30,   279,   290,    -1,   141,
     150,   185,   290,    -1,   141,   181,   185,   290,    -1,   141,
     289,   191,   290,    -1,   141,   135,    -1,   141,    64,   150,
     279,    -1,    74,   279,   291,    -1,    73,   279,   291,    -1,
     141,   193,   287,    -1,   141,   212,   193,    -1,   141,   177,
     288,    -1,   141,   120,   290,    -1,   141,    99,    88,   279,
     257,    -1,   108,     5,   195,     5,    -1,   108,     5,    -1,
      -1,   300,    -1,    87,    66,    -1,    87,    66,    40,    41,
      -1,    90,    -1,   138,    -1,    -1,    -1,    28,     4,    -1,
     167,   205,    -1,    -1,     4,    -1,    64,   161,   293,    -1,
     294,    -1,   293,   195,   294,    -1,   295,    97,    54,   296,
      -1,     4,    -1,     4,    -1,    77,   161,   298,    -1,   295,
      -1,   298,   195,   295,    -1,   140,   121,   300,    24,   296,
      -1,    46,   161,   295,    97,    54,   296,    -1,    87,   295,
      -1,    -1,   128,   161,   303,    -1,   295,   158,   295,    -1,
     302,    -1,   303,   195,   302,    -1,    46,   161,   295,   305,
      -1,   110,    -1,   188,    -1,   170,    -1,    -1,   169,    61,
     143,    -1,    -1,    49,   306,    -1,   145,   157,   307,    -1,
      60,   306,    -1,   134,   306,    -1,    92,   312,   116,   315,
     158,   298,    -1,   313,    -1,   312,   195,   313,    -1,    45,
     314,    -1,    46,    -1,    64,    -1,    64,   161,    -1,    72,
      -1,    77,    -1,    92,   118,    -1,   102,    -1,   160,    -1,
     137,    -1,   129,    -1,   131,    -1,    -1,    34,    -1,   346,
      -1,   132,   312,   317,    88,   298,    -1,   116,   315,    -1,
      -1,   123,   319,    88,   320,    -1,   348,    -1,   251,    -1,
     245,    -1,   215,    -1,   214,    -1,   140,   322,    -1,   323,
      -1,   322,   195,   323,    -1,    14,   324,   205,    -1,   345,
     324,   205,    -1,    90,   345,   324,   205,    -1,   138,   345,
     324,   205,    -1,    91,    42,   345,   324,   205,    -1,   139,
      42,   345,   324,   205,    -1,    13,   324,   205,    -1,   158,
      -1,    24,    -1,    14,    -1,    83,   319,   327,    -1,   162,
     328,    -1,    -1,   325,    -1,   328,   195,   325,    -1,   330,
     123,   319,    -1,    69,    -1,    77,    -1,    46,   150,   279,
     332,    -1,    46,   150,   279,   128,   158,   279,    -1,   333,
      -1,   332,   195,   333,    -1,    43,   334,   222,    -1,    77,
     334,   345,   335,    -1,    46,   334,   345,   336,    -1,   128,
     334,   345,   158,   348,    -1,    62,    -1,    -1,    55,    -1,
     130,    -1,    -1,   140,    20,    10,    -1,    77,    20,    10,
      -1,   140,    71,   202,    -1,    77,    71,    -1,    46,   149,
     140,   338,    -1,   339,    -1,   338,   195,   339,    -1,   345,
      24,   202,   340,   342,   184,    24,   343,   344,    -1,    59,
       4,    -1,    -1,    45,    -1,    -1,   136,    24,   112,    -1,
     136,    24,   144,    -1,   136,    24,    53,    -1,    -1,   180,
      -1,   189,    -1,   173,    -1,   178,    -1,    58,    24,     5,
      -1,   182,    24,     4,   183,    24,     5,    -1,    -1,     3,
      -1,   349,    -1,     3,    -1,   349,    -1,     3,    -1,     3,
      -1,   349,    -1,   171,    -1,   172,    -1,   173,    -1,   174,
      -1,   175,    -1,   176,    -1,   177,    -1,   178,    -1,   179,
      -1,   180,    -1,   181,    -1,   182,    -1,   183,    -1,   184,
      -1,   185,    -1,   186,    -1,   187,    -1,   188,    -1,   189,
      -1,   190,    -1,   191,    -1,   192,    -1,   193,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   216,   216,   225,   232,   239,   240,   241,   242,   243,
     244,   245,   246,   247,   248,   249,   250,   251,   252,   253,
     254,   255,   256,   257,   258,   259,   260,   261,   262,   263,
     264,   265,   275,   279,   286,   288,   294,   303,   304,   305,
     306,   307,   308,   309,   310,   311,   312,   316,   318,   320,
     322,   328,   336,   340,   344,   352,   353,   357,   361,   362,
     363,   364,   365,   366,   367,   370,   371,   375,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   398,   402,   406,   410,   414,   418,
     422,   426,   430,   434,   438,   442,   446,   453,   457,   462,
     470,   471,   475,   477,   482,   489,   490,   494,   508,   536,
     611,   627,   631,   640,   644,   658,   672,   681,   685,   692,
     706,   723,   726,   730,   734,   741,   745,   754,   763,   765,
     767,   769,   771,   773,   782,   791,   793,   795,   797,   802,
     809,   811,   818,   825,   832,   839,   841,   843,   849,   861,
     863,   866,   870,   871,   875,   876,   880,   881,   885,   886,
     890,   893,   897,   902,   907,   909,   911,   916,   920,   925,
     931,   936,   941,   946,   951,   956,   961,   967,   973,   982,
     983,   987,   988,   999,  1018,  1021,  1025,  1029,  1031,  1036,
    1043,  1048,  1060,  1070,  1071,  1076,  1080,  1094,  1107,  1119,
    1129,  1144,  1149,  1157,  1162,  1166,  1167,  1174,  1178,  1184,
    1185,  1199,  1201,  1206,  1207,  1211,  1215,  1220,  1230,  1251,
    1272,  1297,  1298,  1302,  1327,  1348,  1369,  1394,  1395,  1399,
    1406,  1414,  1422,  1426,  1430,  1442,  1445,  1459,  1463,  1468,
    1474,  1478,  1483,  1488,  1490,  1495,  1497,  1503,  1504,  1510,
    1511,  1519,  1526,  1533,  1540,  1547,  1558,  1569,  1584,  1585,
    1592,  1593,  1597,  1604,  1606,  1611,  1619,  1620,  1622,  1628,
    1629,  1637,  1640,  1644,  1651,  1656,  1664,  1672,  1682,  1686,
    1693,  1695,  1700,  1704,  1708,  1712,  1716,  1720,  1724,  1733,
    1741,  1745,  1749,  1758,  1764,  1770,  1776,  1783,  1784,  1794,
    1802,  1803,  1804,  1805,  1809,  1810,  1820,  1825,  1827,  1829,
    1831,  1833,  1838,  1840,  1842,  1844,  1846,  1850,  1863,  1867,
    1871,  1878,  1887,  1897,  1901,  1903,  1905,  1910,  1911,  1912,
    1917,  1918,  1920,  1926,  1927,  1937,  1943,  1947,  1953,  1959,
    1965,  1977,  1983,  1987,  1999,  2003,  2009,  2014,  2024,  2030,
    2036,  2040,  2051,  2057,  2062,  2075,  2080,  2084,  2089,  2093,
    2099,  2110,  2122,  2134,  2145,  2149,  2155,  2161,  2166,  2171,
    2176,  2181,  2186,  2191,  2196,  2201,  2206,  2213,  2218,  2223,
    2228,  2239,  2279,  2284,  2295,  2302,  2307,  2309,  2311,  2313,
    2324,  2332,  2336,  2343,  2349,  2356,  2363,  2370,  2377,  2384,
    2393,  2394,  2398,  2409,  2416,  2421,  2426,  2430,  2443,  2451,
    2453,  2464,  2470,  2481,  2485,  2492,  2497,  2503,  2508,  2517,
    2518,  2522,  2523,  2524,  2528,  2533,  2538,  2542,  2556,  2564,
    2568,  2575,  2590,  2593,  2597,  2600,  2604,  2606,  2608,  2611,
    2615,  2620,  2625,  2630,  2638,  2642,  2647,  2658,  2660,  2677,
    2679,  2696,  2700,  2702,  2715,  2716,  2717,  2718,  2719,  2720,
    2721,  2722,  2723,  2724,  2725,  2726,  2727,  2728,  2729,  2730,
    2731,  2732,  2733,  2734,  2735,  2736,  2737
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "NAME", "STRING", "INTNUM", "DATE_VALUE",
  "HINT_VALUE", "BOOL", "APPROXNUM", "NULLX", "UNKNOWN", "QUESTIONMARK",
  "SYSTEM_VARIABLE", "TEMP_VARIABLE", "EXCEPT", "UNION", "INTERSECT", "OR",
  "AND", "NOT", "COMP_NE", "COMP_GE", "COMP_GT", "COMP_EQ", "COMP_LT",
  "COMP_LE", "CNNOP", "LIKE", "BETWEEN", "IN", "IS", "'+'", "'-'", "'*'",
  "'/'", "'%'", "MOD", "'^'", "UMINUS", "'('", "')'", "'.'", "ADD", "ANY",
  "ALL", "ALTER", "AS", "ASC", "BEGI", "BIGINT", "BINARY", "BOOLEAN",
  "BOTH", "BY", "CASCADE", "CASE", "CHARACTER", "CLUSTER", "COMMENT",
  "COMMIT", "CONSISTENT", "COLUMN", "COLUMNS", "CREATE", "CREATETIME",
  "CURRENT_USER", "DATE", "DATETIME", "DEALLOCATE", "DECIMAL", "DEFAULT",
  "DELETE", "DESC", "DESCRIBE", "DISTINCT", "DOUBLE", "DROP", "DUAL",
  "ELSE", "END", "END_P", "ERROR", "EXECUTE", "EXISTS", "EXPLAIN", "FLOAT",
  "FOR", "FROM", "FULL", "GLOBAL", "GLOBAL_ALIAS", "GRANT", "GROUP",
  "HAVING", "HINT_BEGIN", "HINT_END", "IDENTIFIED", "IF", "INDEX", "INNER",
  "INTEGER", "INSERT", "INTO", "JOIN", "KEY", "LEADING", "LEFT", "LIMIT",
  "LOCAL", "LOCKED", "MEDIUMINT", "MEMORY", "MODIFYTIME", "NUMERIC",
  "OFFSET", "ON", "ORDER", "OPTION", "OUTER", "PARAMETERS", "PASSWORD",
  "PRECISION", "PREPARE", "PRIMARY", "READ_CONSISTENCY", "READ_STATIC",
  "REAL", "RENAME", "REPLACE", "RESTRICT", "PRIVILEGES", "REVOKE", "RIGHT",
  "ROLLBACK", "SCHEMA", "SCOPE", "SELECT", "SESSION", "SESSION_ALIAS",
  "SET", "SHOW", "SMALLINT", "SNAPSHOT", "SPFILE", "START", "STATIC",
  "STORING", "STRONG", "SYSTEM", "TABLE", "TABLES", "THEN", "TIME",
  "TIMESTAMP", "TINYINT", "TRAILING", "TRANSACTION", "TO", "UNIQUE",
  "UPDATE", "USER", "USING", "VALUES", "VARCHAR", "VARBINARY", "WEAK",
  "WHERE", "WHEN", "WITH", "WORK", "AUTO_INCREMENT", "CHARSET",
  "CHUNKSERVER", "COMPRESS_METHOD", "CONSISTENT_MODE", "EXPIRE_INFO",
  "GRANTS", "MERGESERVER", "REPLICA_NUM", "ROOTSERVER", "SERVER",
  "SERVER_IP", "SERVER_PORT", "SERVER_TYPE", "STATUS", "TABLET_BLOCK_SIZE",
  "TABLET_MAX_SIZE", "UNLOCKED", "UPDATESERVER", "USE_BLOOM_FILTER",
  "VARIABLES", "VERBOSE", "WARNINGS", "';'", "','", "$accept", "sql_stmt",
  "stmt_list", "stmt", "expr_list", "column_ref", "expr_const",
  "simple_expr", "arith_expr", "expr", "in_expr", "case_expr", "case_arg",
  "when_clause_list", "when_clause", "case_default", "func_expr",
  "distinct_or_all", "delete_stmt", "update_stmt", "update_asgn_list",
  "update_asgn_factor", "create_table_stmt", "opt_if_not_exists",
  "table_element_list", "table_element", "column_definition", "data_type",
  "opt_decimal", "opt_float", "opt_precision", "opt_time_precision",
  "opt_char_length", "opt_column_attribute_list", "column_attribute",
  "opt_table_option_list", "table_option", "opt_equal_mark",
  "opt_default_mark", "create_index_stmt", "opt_unique", "index_name",
  "sort_column_list", "sort_column_key", "opt_storing", "drop_table_stmt",
  "opt_if_exists", "table_list", "drop_index_stmt", "insert_stmt",
  "replace_or_insert", "opt_insert_columns", "column_list",
  "insert_vals_list", "insert_vals", "select_stmt", "select_with_parens",
  "select_no_parens", "no_table_select", "select_clause", "simple_select",
  "opt_where", "select_limit", "opt_hint", "opt_hint_list", "hint_option",
  "consistent_level", "limit_expr", "opt_select_limit", "opt_for_update",
  "parameterized_trim", "opt_groupby", "opt_order_by", "order_by",
  "sort_list", "sort_key", "opt_asc_desc", "opt_having", "opt_distinct",
  "projection", "select_expr_list", "from_list", "table_factor",
  "relation_factor", "joined_table", "join_type", "join_outer",
  "explain_stmt", "explainable_stmt", "opt_verbose", "show_stmt",
  "opt_limit", "opt_for_grant_user", "opt_scope", "opt_show_condition",
  "opt_like_condition", "create_user_stmt", "user_specification_list",
  "user_specification", "user", "password", "drop_user_stmt", "user_list",
  "set_password_stmt", "opt_for_user", "rename_user_stmt", "rename_info",
  "rename_list", "lock_user_stmt", "lock_spec", "opt_work",
  "opt_with_consistent_snapshot", "begin_stmt", "commit_stmt",
  "rollback_stmt", "grant_stmt", "priv_type_list", "priv_type",
  "opt_privilege", "priv_level", "revoke_stmt", "opt_on_priv_level",
  "prepare_stmt", "stmt_name", "preparable_stmt", "variable_set_stmt",
  "var_and_val_list", "var_and_val", "to_or_eq", "argument",
  "execute_stmt", "opt_using_args", "argument_list",
  "deallocate_prepare_stmt", "deallocate_or_drop", "alter_table_stmt",
  "alter_column_actions", "alter_column_action", "opt_column",
  "opt_drop_behavior", "alter_column_behavior", "alter_system_stmt",
  "alter_system_actions", "alter_system_action", "opt_comment", "opt_all",
  "opt_config_scope", "server_type", "opt_cluster_or_address",
  "column_name", "relation_name", "function_name", "column_label",
  "unreserved_keyword", 0
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,    43,    45,    42,    47,    37,   287,    94,   288,
      40,    41,    46,   289,   290,   291,   292,   293,   294,   295,
     296,   297,   298,   299,   300,   301,   302,   303,   304,   305,
     306,   307,   308,   309,   310,   311,   312,   313,   314,   315,
     316,   317,   318,   319,   320,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,   347,   348,   349,   350,   351,   352,   353,   354,   355,
     356,   357,   358,   359,   360,   361,   362,   363,   364,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   430,   431,   432,   433,   434,   435,
     436,   437,   438,   439,    59,    44
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   196,   197,   198,   198,   199,   199,   199,   199,   199,
     199,   199,   199,   199,   199,   199,   199,   199,   199,   199,
     199,   199,   199,   199,   199,   199,   199,   199,   199,   199,
     199,   199,   200,   200,   201,   201,   201,   202,   202,   202,
     202,   202,   202,   202,   202,   202,   202,   203,   203,   203,
     203,   203,   203,   203,   203,   204,   204,   204,   204,   204,
     204,   204,   204,   204,   204,   205,   205,   205,   205,   205,
     205,   205,   205,   205,   205,   205,   205,   205,   205,   205,
     205,   205,   205,   205,   205,   205,   205,   205,   205,   205,
     205,   205,   205,   205,   205,   205,   205,   206,   206,   207,
     208,   208,   209,   209,   210,   211,   211,   212,   212,   212,
     212,   212,   212,   213,   213,   214,   215,   216,   216,   217,
     218,   219,   219,   220,   220,   221,   221,   222,   223,   223,
     223,   223,   223,   223,   223,   223,   223,   223,   223,   223,
     223,   223,   223,   223,   223,   223,   223,   223,   223,   224,
     224,   224,   225,   225,   226,   226,   227,   227,   228,   228,
     229,   229,   230,   230,   230,   230,   230,   231,   231,   231,
     232,   232,   232,   232,   232,   232,   232,   232,   232,   233,
     233,   234,   234,   235,   236,   236,   237,   238,   238,   239,
     240,   240,   241,   242,   242,   243,   243,   244,   245,   245,
     245,   246,   246,   247,   247,   248,   248,   249,   249,   250,
     250,   251,   251,   252,   252,   253,   253,   253,   253,   254,
     254,   255,   255,   256,   256,   256,   256,   257,   257,   257,
     258,   258,   258,   258,   258,   259,   259,   260,   260,   260,
     261,   261,   261,   262,   262,   263,   263,   264,   264,   265,
     265,   266,   266,   266,   266,   266,   266,   266,   267,   267,
     268,   268,   269,   270,   270,   271,   272,   272,   272,   273,
     273,   274,   274,   274,   275,   275,   275,   275,   276,   276,
     277,   277,   278,   278,   278,   278,   278,   278,   278,   279,
     280,   280,   280,   281,   281,   281,   281,   282,   282,   283,
     284,   284,   284,   284,   285,   285,   286,   286,   286,   286,
     286,   286,   286,   286,   286,   286,   286,   286,   286,   286,
     286,   287,   287,   287,   288,   288,   288,   289,   289,   289,
     290,   290,   290,   291,   291,   292,   293,   293,   294,   295,
     296,   297,   298,   298,   299,   299,   300,   300,   301,   302,
     303,   303,   304,   305,   305,   306,   306,   307,   307,   308,
     308,   309,   310,   311,   312,   312,   313,   313,   313,   313,
     313,   313,   313,   313,   313,   313,   313,   314,   314,   315,
     315,   316,   317,   317,   318,   319,   320,   320,   320,   320,
     321,   322,   322,   323,   323,   323,   323,   323,   323,   323,
     324,   324,   325,   326,   327,   327,   328,   328,   329,   330,
     330,   331,   331,   332,   332,   333,   333,   333,   333,   334,
     334,   335,   335,   335,   336,   336,   336,   336,   337,   338,
     338,   339,   340,   340,   341,   341,   342,   342,   342,   342,
     343,   343,   343,   343,   344,   344,   344,   345,   345,   346,
     346,   347,   348,   348,   349,   349,   349,   349,   349,   349,
     349,   349,   349,   349,   349,   349,   349,   349,   349,   349,
     349,   349,   349,   349,   349,   349,   349
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     3,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     0,     1,     3,     1,     3,     3,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     3,     1,     1,     3,
       5,     1,     1,     1,     2,     1,     2,     2,     3,     3,
       3,     3,     3,     3,     3,     1,     2,     2,     3,     3,
       3,     3,     3,     3,     3,     3,     3,     3,     3,     3,
       3,     3,     4,     3,     3,     2,     3,     4,     3,     4,
       3,     4,     5,     6,     3,     4,     3,     1,     3,     5,
       1,     0,     1,     2,     4,     2,     0,     4,     5,     4,
       6,     4,     3,     1,     1,     4,     5,     1,     3,     3,
       8,     3,     0,     1,     3,     1,     5,     3,     1,     1,
       1,     1,     1,     2,     2,     1,     2,     1,     2,     2,
       1,     2,     2,     2,     2,     1,     1,     1,     2,     5,
       3,     0,     3,     0,     1,     0,     3,     0,     3,     0,
       2,     0,     2,     1,     2,     1,     2,     1,     3,     0,
       3,     3,     3,     3,     3,     3,     3,     4,     5,     1,
       0,     1,     0,    11,     1,     0,     1,     1,     3,     2,
       4,     0,     4,     0,     2,     1,     3,     3,     6,     4,
       7,     1,     1,     3,     0,     1,     3,     3,     5,     1,
       3,     1,     1,     3,     3,     1,     2,     3,     4,     5,
       8,     1,     1,     9,     4,     4,     4,     0,     2,     3,
       4,     4,     2,     2,     4,     0,     3,     1,     3,     0,
       1,     4,     2,     1,     1,     1,     1,     0,     1,     0,
       2,     3,     4,     4,     4,     3,     3,     3,     0,     3,
       1,     0,     3,     1,     3,     2,     0,     1,     1,     0,
       2,     0,     1,     1,     1,     2,     3,     1,     1,     3,
       1,     3,     1,     3,     2,     3,     2,     1,     5,     1,
       3,     6,     5,     2,     2,     2,     1,     1,     0,     3,
       1,     1,     1,     1,     1,     0,     4,     5,     5,     4,
       4,     4,     2,     4,     3,     3,     3,     3,     3,     3,
       5,     4,     2,     0,     1,     2,     4,     1,     1,     0,
       0,     2,     2,     0,     1,     3,     1,     3,     4,     1,
       1,     3,     1,     3,     5,     6,     2,     0,     3,     3,
       1,     3,     4,     1,     1,     1,     0,     3,     0,     2,
       3,     2,     2,     6,     1,     3,     2,     1,     1,     2,
       1,     1,     2,     1,     1,     1,     1,     1,     0,     1,
       1,     5,     2,     0,     4,     1,     1,     1,     1,     1,
       2,     1,     3,     3,     3,     4,     4,     5,     5,     3,
       1,     1,     1,     3,     2,     0,     1,     3,     3,     1,
       1,     4,     6,     1,     3,     3,     4,     4,     5,     1,
       0,     1,     1,     0,     3,     3,     3,     2,     4,     1,
       3,     9,     2,     0,     1,     0,     3,     3,     3,     0,
       1,     1,     1,     1,     3,     6,     0,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
      31,     0,     0,   356,   356,   185,   409,     0,     0,     0,
     410,     0,   305,     0,   202,     0,     0,   201,     0,   356,
     235,     0,   329,     0,     0,     0,     0,     4,     9,     8,
       7,    29,    10,    30,     6,     0,     5,   222,   211,   215,
     261,   221,    11,    12,    19,    20,    21,    22,    23,    26,
      27,    28,    24,    25,    13,    14,    15,    18,     0,    16,
      17,   222,     0,     0,     0,     0,   355,   359,   361,   122,
     184,     0,     0,     0,   449,   454,   455,   456,   457,   458,
     459,   460,   461,   462,   463,   464,   465,   466,   467,   468,
     469,   470,   471,   472,   473,   474,   475,   476,   333,   289,
     450,   333,     0,   193,     0,   452,   405,   385,   453,   304,
       0,   378,   367,   368,   370,   371,     0,   373,   376,   375,
     374,     0,   364,     0,     0,   383,   362,   239,   271,   447,
       0,     0,     0,     0,   347,     0,     0,   390,   391,     0,
     448,   451,   434,     0,     0,   327,     0,   330,   312,   328,
       0,   347,     0,   323,     0,     0,     0,     0,   358,     0,
       1,     2,    31,     0,   271,   271,   271,     0,     0,   249,
       0,   216,     0,   214,   213,     0,     0,   339,     0,     0,
       0,   335,   336,     0,     0,   227,   334,   315,   314,     0,
       0,   197,   282,   287,     0,     0,   342,   341,     0,   403,
     301,   303,   302,   300,   299,   377,   366,   369,   372,     0,
       0,     0,     0,   350,   348,     0,     0,     0,     0,   240,
       0,   237,   272,   273,     0,   401,   400,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   319,   330,     0,   318,   324,   330,     0,
     316,   317,   330,   330,     0,     0,   360,     0,     3,   204,
       0,     0,     0,     0,     0,     0,   249,   217,   250,   408,
     428,   429,     0,   420,   420,   420,   420,   411,   413,     0,
     353,   354,   352,     0,     0,     0,     0,     0,   186,     0,
     115,   222,     0,   287,     0,   286,   298,   296,     0,   298,
     298,     0,     0,   284,   194,   192,   195,     0,   402,   406,
     404,   379,     0,   380,   365,   389,   388,   387,   386,   384,
       0,     0,   382,     0,   242,     0,   236,     0,   447,    37,
      39,    38,    41,    40,    42,    43,    45,    44,     0,     0,
       0,   277,     0,   101,     0,     0,    47,    48,    65,   274,
      51,    52,    53,   278,   247,    34,     0,   448,   399,   393,
       0,     0,   346,     0,     0,     0,   392,   394,   330,   330,
     313,   227,   331,   332,   309,   325,   310,   322,   311,   306,
       0,   112,   113,     0,   114,     0,     0,     0,    32,     0,
       0,     0,   227,   117,     0,     0,     0,   199,   235,   222,
     226,   221,   224,   225,   266,   262,   263,   245,   246,   232,
     233,   218,     0,     0,   419,     0,     0,     0,     0,     0,
       0,     0,   121,     0,   337,     0,     0,     0,   228,   290,
     285,   297,   293,     0,   294,   295,     0,   283,     0,   343,
       0,     0,   349,   351,   381,   244,   243,     0,   238,    85,
      66,    67,     0,    32,    53,   100,     0,    54,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   275,     0,     0,   248,   219,     0,   395,     0,   340,
     344,   396,     0,   308,   307,   320,     0,     0,   107,     0,
       0,     0,     0,     0,     0,   109,     0,     0,     0,     0,
     111,   357,     0,   116,     0,     0,   205,     0,   271,   267,
     268,   265,     0,     0,     0,     0,   430,   433,   415,     0,
       0,   423,   412,     0,   420,   414,   345,     0,     0,   123,
     125,   338,     0,   229,     0,     0,     0,   196,   407,   363,
     241,     0,    49,     0,   106,   102,    46,    84,    83,     0,
       0,     0,    80,    78,    79,    77,    76,    75,    96,    81,
       0,     0,    55,     0,     0,    94,    97,    88,    86,    90,
       0,    68,    69,    70,    71,    72,    74,    73,   276,   227,
     227,   280,   279,    36,    35,   397,   398,   326,   321,   255,
       0,   256,     0,   257,     0,    33,   132,   159,   135,   159,
     145,   147,   140,   151,   155,   153,   131,   130,   146,   151,
     137,   129,   157,   157,   128,   159,   159,     0,   251,   108,
     118,   119,   203,     0,     0,   198,     0,   264,   230,   234,
     231,     0,   439,   161,     0,     0,   417,   421,   422,   416,
       0,     0,   169,     0,     0,   288,   292,     0,    33,     0,
       0,   103,     0,    82,     0,    95,    56,    57,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    32,    89,    87,
      91,   247,     0,   258,   252,   253,   254,     0,   142,   141,
       0,   133,   154,   138,     0,   136,   134,     0,   148,   139,
     143,   144,   110,   200,   206,   209,     0,     0,     0,   432,
       0,     0,   127,     0,   427,     0,     0,   418,     0,   181,
     180,   180,   180,   180,   180,   180,   180,   120,   167,     0,
     124,     0,   187,   266,   291,    50,     0,   105,    99,     0,
      92,    58,    59,    60,    61,    62,    64,    63,    98,   220,
     281,     0,   269,     0,     0,     0,     0,   207,     0,     0,
       0,     0,     0,   163,     0,     0,     0,   165,   160,   425,
     424,   426,     0,   179,     0,     0,     0,     0,     0,     0,
       0,   182,     0,   180,   191,     0,   189,   104,    93,     0,
       0,   223,   158,   150,     0,   152,   156,   210,     0,   438,
     436,   437,     0,   162,   164,   166,   126,   174,   176,   170,
     173,   172,   171,   175,   168,   180,     0,     0,   169,   188,
     259,   270,     0,   208,   442,   443,   440,   441,   446,     0,
     177,     0,   183,   149,     0,     0,   431,   178,     0,     0,
       0,   190,   444,     0,     0,     0,   445
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    25,    26,    27,   387,   346,   347,   348,   573,   349,
     575,   350,   456,   554,   555,   662,   351,   389,    28,    29,
     392,   393,    30,   180,   538,   539,   540,   627,   691,   695,
     693,   698,   688,   712,   768,   727,   728,   774,   729,    31,
      72,   287,   731,   732,   818,    32,   195,   305,    33,    34,
      35,   396,   515,   635,   706,    36,   352,    38,    39,    40,
      41,   290,   484,   128,   220,   221,   447,   409,   485,   171,
     390,   752,   168,   169,   405,   406,   521,   791,   224,   353,
     354,   590,   591,   192,   193,   301,   432,    42,   204,   110,
      43,   250,   246,   155,   243,   187,    44,   181,   182,   196,
     490,    45,   197,    46,   232,    47,   213,   214,    48,   282,
      67,   256,    49,    50,    51,    52,   121,   122,   206,   312,
      53,   216,    54,   106,   319,    55,   137,   138,   227,   309,
      56,   199,   310,    57,    58,    59,   277,   278,   419,   649,
     646,    60,   270,   271,   642,   156,   711,   828,   836,   355,
     356,   157,   107,   357
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -667
static const yytype_int16 yypact[] =
{
     498,     7,   242,  -106,  -106,   -76,  -667,    22,  2179,  2179,
     -42,  2202,  -101,   381,  -667,  2202,    50,  -667,   381,  -106,
      28,  1839,   371,    21,  2179,   212,   -38,  -667,  -667,  -667,
    -667,  -667,  -667,  -667,  -667,   117,  -667,   -31,  -667,  -667,
      83,     5,  -667,  -667,  -667,  -667,  -667,  -667,  -667,  -667,
    -667,  -667,  -667,  -667,  -667,  -667,  -667,  -667,   105,  -667,
    -667,   195,   200,   128,  2179,   274,  -667,  -667,  -667,   186,
    -667,   274,   189,  2179,  -667,  -667,  -667,  -667,  -667,  -667,
    -667,  -667,  -667,  -667,  -667,  -667,  -667,  -667,  -667,  -667,
    -667,  -667,  -667,  -667,  -667,  -667,  -667,  -667,   275,  -667,
    -667,   275,  1865,   204,   274,  -667,   161,  -667,  -667,  -667,
     185,   217,  -667,   194,  -667,  -667,   228,  -667,  -667,  -667,
    -667,   -74,  -667,   247,   274,   -43,  -667,   174,    15,  -667,
      31,    31,  2247,   308,   267,  2247,   315,   170,  -667,    31,
    -667,  -667,  -667,    32,   220,  -667,   284,     4,  -667,  -667,
     192,   302,   211,   293,   219,   214,   262,   377,   256,   289,
    -667,  -667,   498,  2179,    15,    15,    15,   379,   218,   193,
     280,  -667,  2202,  -667,  -667,  2247,   291,  -667,   159,   422,
    2179,   248,  -667,   354,  2179,   292,  -667,  -667,  -667,  1124,
    1952,   290,  1986,  -667,   376,  1865,  -667,   273,   443,  -667,
    -667,  -667,  -667,  -667,  -667,  -667,  -667,  -667,  -667,  2112,
     381,   185,   313,  -667,   282,  2112,   387,   438,   441,  -667,
     -48,  -667,  -667,  -667,  1097,  -667,  -667,  1705,  1705,    31,
    2247,   274,   464,    31,  2247,  1896,  1705,  2179,  2179,  2179,
    2179,   485,  1705,  -667,     4,    64,  -667,  -667,     4,   487,
    -667,  -667,     4,     4,   830,   432,  -667,  2247,  -667,    29,
      37,    37,    37,  1705,   277,   277,   412,  -667,  -667,  -667,
     305,  -667,   479,   445,   445,   445,    51,   310,  -667,   454,
    -667,  -667,  -667,   428,   471,   274,   460,   403,  -667,  1153,
    -667,  1375,   290,   482,  2179,  -667,   406,  -667,  1865,   406,
     406,   424,  2179,  -667,  -667,   337,   290,   274,  -667,  -667,
     338,  -667,   384,  -667,  -667,  -667,  -667,  -667,  -667,  -667,
     274,   274,  -667,   274,  -667,   -73,  -667,   174,    65,  -667,
    -667,  -667,  -667,  -667,  -667,  -667,  -667,  -667,  1705,  1705,
    1705,  -667,  1344,  1705,   495,   497,  -667,  -667,  -667,  1042,
    -667,  -667,  -667,  -667,   236,  -667,   503,   507,  2503,  2503,
    1705,    31,  -667,   533,  1705,    31,  -667,  2503,     4,     4,
    -667,   292,  -667,  2503,  -667,   513,  -667,   359,  -667,  -667,
     514,  -667,  -667,  1400,  -667,  1457,  1648,   -22,  1006,  1705,
     516,   418,   -89,  -667,   539,  1432,   405,  -667,    28,  -667,
     549,  -667,   549,  -667,  2423,   374,  -667,  -667,  -667,    10,
     465,  -667,  2247,   307,  -667,  2247,  2247,  2247,  2179,  2247,
     316,   533,  -667,  2056,  -667,   533,  2179,  1705,  2503,   527,
    -667,  -667,  -667,   413,  -667,  -667,  1865,  -667,  1865,  -667,
     443,   274,  -667,  -667,   273,  -667,  -667,   535,  -667,  2522,
    -667,  -667,   383,  2236,    87,  2503,   411,  -667,  2247,  1705,
    1705,   392,  1705,  1705,  1705,  1705,  1705,  1705,  1705,  1705,
    1761,   540,   365,  1705,  1705,  1705,  1705,  1705,  1705,  1705,
    2202,  -667,  1919,  1097,  -667,  -667,  2147,  2503,  1705,  -667,
    -667,  2503,  1705,  -667,  -667,  -667,   543,   580,  -667,  1705,
    2165,  1705,  2188,  1705,  2378,  -667,  1705,   602,  1705,  2454,
    -667,  -667,  2247,  -667,  1705,   -18,  -667,   547,    15,  -667,
    -667,  -667,  1705,   277,   277,   277,  -667,   529,  -667,   602,
     -24,   180,  -667,   431,   445,  -667,  -667,   486,   -16,  -667,
    -667,  -667,   552,  2503,  2179,  1705,   427,   290,  -667,   273,
    -667,  1705,  -667,  1705,    47,  -667,  -667,   918,  2522,  1705,
    1761,   540,   960,   960,   960,   960,   960,   960,   823,   701,
    1761,  1761,  -667,   658,  1344,  -667,  -667,  -667,  -667,  -667,
     373,   560,   560,   561,   561,   561,   561,  -667,  -667,   292,
     -66,   290,  -667,  -667,  -667,  2503,  2503,  -667,  -667,  2503,
    1705,  2503,  1705,  2503,  1705,  2503,  -667,   563,  -667,   563,
    -667,  -667,  -667,   564,   489,   573,  -667,  -667,  -667,   564,
    -667,  -667,   574,   574,  -667,   563,   563,   576,  2503,  -667,
    -667,  2503,     7,  2247,  1705,   420,  1097,  -667,  -667,  -667,
    -667,   597,   483,  -667,    43,   203,  -667,  -667,  -667,  -667,
    2202,   582,   153,  2056,  2247,  -667,  2503,  1705,  2479,  1220,
    1705,  -667,   544,   701,   685,  -667,  -667,  -667,  1761,  1761,
    1761,  1761,  1761,  1761,  1761,  1761,   -15,  2503,  -667,  -667,
    -667,   218,  1865,   530,  2503,  2503,  2503,   615,  -667,  -667,
     623,  -667,  -667,  -667,   624,  -667,  -667,   626,  -667,  -667,
    -667,  -667,  -667,  -667,  -667,  2503,   -12,   593,    -4,  -667,
     610,   456,    17,   631,  -667,   634,   307,  -667,  2247,  -667,
     625,   625,   625,   625,   625,   625,   625,   452,  -667,   -29,
    -667,   -10,  -667,    39,  2503,  -667,  1705,  2503,  -667,  1761,
     726,   572,   572,   612,   612,   612,   612,  -667,  -667,  -667,
     290,   594,   557,   614,    -5,   616,   619,  -667,  1705,  1705,
    1865,   163,   632,  -667,   654,   307,   566,  -667,  -667,  -667,
    -667,  -667,    -3,  -667,   664,   534,   669,   670,   674,   676,
     677,   311,   542,   625,   537,  2247,  -667,  2503,   726,  1705,
    1705,  -667,  -667,  -667,   681,  -667,  -667,  2503,    -2,  -667,
    -667,  -667,   191,  -667,  -667,  -667,  -667,  -667,  -667,  -667,
    -667,  -667,  -667,  -667,  -667,   625,   693,   649,   153,  -667,
     504,  2503,   657,  -667,  -667,  -667,  -667,  -667,   -37,   696,
    -667,  2247,   452,  -667,   678,   683,  -667,  -667,    -1,   700,
     707,  -667,  -667,   531,   703,   719,  -667
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -667,  -667,  -667,   579,  -337,  -667,  -407,  -409,  -536,   323,
     164,  -667,  -667,  -667,   188,  -667,   706,  -667,   -69,   -61,
    -667,   231,  -667,  -667,  -667,    92,   331,   222,   129,  -667,
    -667,   124,  -333,  -667,  -667,   -65,   -32,  -259,  -667,  -667,
    -667,  -667,  -667,   -20,  -667,  -667,  -667,  -667,  -667,   -52,
    -667,  -667,  -666,  -667,    11,  -102,    44,    19,  -667,   176,
     187,  -338,   586,   375,  -667,   442,  -667,  -253,    90,   -98,
    -667,  -667,  -667,  -667,  -667,   250,    41,  -667,  -155,   295,
     139,  -667,  -100,     6,   587,  -667,  -203,  -667,  -667,  -667,
    -667,  -667,  -667,  -667,  -172,   679,  -667,  -667,   494,    46,
    -167,  -667,  -301,  -667,   630,  -667,   463,  -667,  -667,  -667,
     215,  -667,  -667,  -667,  -667,  -667,   775,   584,  -667,   588,
    -667,  -667,  -667,     3,  -667,  -667,  -667,   570,   -80,   355,
    -667,  -667,  -667,  -667,  -667,  -667,  -667,   386,   181,  -667,
    -667,  -667,  -667,   395,  -667,  -667,  -667,  -667,  -667,   -17,
      58,  -667,  -332,    -8
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -452
static const yytype_int16 yytable[] =
{
     100,   100,   191,   108,   139,   452,   527,   108,   203,   260,
     261,   262,   410,   140,    98,   101,   100,   481,   123,   505,
      62,   834,   444,   632,   664,   652,   748,   763,   782,   757,
     159,   784,   241,   495,   666,   667,   793,   764,   806,   823,
     841,   200,   209,   161,    37,    61,  -249,     1,   326,   201,
    -212,   228,   772,   644,   513,   225,   100,   102,   202,   236,
     222,   572,   237,   713,    66,   100,    99,    99,   177,   395,
     176,   267,   374,   215,    69,   445,   376,     1,   289,   185,
     378,   379,    99,    70,   760,    71,  -249,   519,   765,   292,
     223,   109,   170,   446,   100,   306,   434,   435,   164,   165,
     166,   289,  -222,  -222,  -222,  -451,   512,  -449,   103,   318,
      73,   178,   520,   414,   714,   229,   645,   183,   233,   104,
     238,   210,    99,   127,   140,   523,   660,   140,   173,   682,
     375,    99,   740,   741,   742,   743,   744,   745,   746,   747,
     549,   766,   315,   783,    20,   835,   190,   327,   588,   360,
     316,   572,   210,   364,    37,   100,   162,   397,   272,   317,
      99,   572,   572,  -212,   108,   838,    20,   140,   411,   259,
     212,   242,   100,   506,   398,   269,   100,   633,   158,   653,
     506,   100,   100,   758,   100,   785,   284,   100,   767,   226,
     794,   483,   633,   758,   633,  -222,   493,   494,   433,  -249,
     167,   100,  -222,   788,  -222,   524,    37,   100,    62,   418,
    -182,   124,   160,   361,   217,   553,   799,   365,   139,    68,
     163,    99,   140,   715,   719,     1,   140,   140,   172,   100,
     100,   100,   100,   291,   126,   647,   173,   676,    99,   190,
     394,   174,   288,   368,   369,   370,   371,    99,   295,   140,
     303,   681,   683,    99,   536,    37,   279,     7,   541,   572,
     572,   572,   572,   572,   572,   572,   572,   313,   175,   280,
     638,   639,   640,   313,   716,   800,   689,   362,   177,   186,
     170,   488,   407,   100,   179,   492,   100,    14,   184,   408,
     100,   362,   700,   701,   100,    99,    99,    99,    99,   218,
     219,  -260,   194,    37,   399,   399,   399,   801,  -260,   771,
     648,   329,   330,   331,    17,   332,   333,   334,   717,   335,
     336,   337,    20,   198,   482,  -182,   264,   720,   721,   722,
     572,   183,   723,   265,   273,   211,   546,   274,   547,   724,
     725,   108,   190,   726,   264,    24,   208,   281,   205,   295,
     230,   265,   430,   439,   231,   207,    99,   234,   804,   273,
     437,    62,   274,   636,   824,   235,   442,   212,   275,   825,
     239,   826,   240,   577,   141,   578,   579,   244,   516,   296,
     827,   678,   719,   679,   680,   580,   454,   140,   457,   245,
     297,    63,    64,   275,   298,   272,   248,   299,   529,   530,
     531,   249,   533,    65,   140,   252,   529,   140,   140,   140,
     100,   140,   251,   253,    62,   140,   142,   254,   100,   276,
     559,   560,   561,   300,   532,   255,   111,   112,   100,   257,
     100,   483,   542,   263,   143,   144,   400,   402,   403,    61,
     268,   556,   283,   285,   534,   113,   345,   401,   401,   401,
     140,   286,   820,   114,   415,   416,   417,   308,   115,   289,
     304,   145,   775,   776,   777,   778,   779,   780,   307,   594,
     146,   320,   108,   116,   100,   323,    99,   321,   140,   324,
     190,   325,   190,   117,    99,   720,   721,   722,   363,   372,
     723,   147,   377,   391,    99,   394,    99,   724,   725,   170,
     412,   726,   296,   413,   140,   420,   148,   414,   421,   149,
     118,   423,   422,   297,   425,   576,   296,   298,   119,   426,
     299,   150,  -435,   429,   816,   431,   190,   297,   436,   545,
     703,   298,   438,   440,   299,     1,   100,   489,     1,   458,
      99,   120,   441,   657,     2,   486,   300,     3,   151,  -450,
     358,   359,   152,   496,   497,   498,   829,   510,     4,   367,
     300,   511,     5,   514,   153,   373,   166,     6,   517,   522,
       7,     8,     9,   525,   544,    10,   550,   388,   551,   553,
     574,    11,   750,    12,   597,   598,   404,   634,   641,   650,
      13,   651,   654,    62,   475,   476,   477,   478,   479,   479,
      14,   709,   655,   687,   690,   576,   671,   672,   673,   674,
     675,   692,   428,   694,   697,   707,   704,   702,   454,   710,
     753,    15,   718,   751,   738,   140,    16,    17,   754,   755,
      18,   756,    19,   759,   761,    20,   529,   733,    21,    22,
     762,   769,   108,    23,   770,   140,   140,   781,   789,   773,
     675,   790,   606,   607,   608,   792,   802,   795,    24,   609,
     796,   449,   450,   451,   803,   453,   455,   610,   807,   611,
     612,   805,   613,   809,   100,   810,    37,   668,   614,   811,
     808,   812,   815,   487,   817,   813,   822,   491,   615,   831,
     669,   670,   671,   672,   673,   674,   675,   830,   833,   506,
     837,   516,   839,   616,   739,   842,   500,   840,   502,   504,
     140,   843,   509,   617,   844,   618,   619,   669,   670,   671,
     672,   673,   674,   675,   846,   665,   190,   845,   154,   620,
     470,   471,   472,   473,   474,   475,   476,   477,   478,   479,
      99,   258,   661,   630,   621,   730,   528,   699,   696,   814,
     543,   643,   100,   832,   266,   622,   623,   624,   669,   670,
     671,   672,   673,   674,   675,   819,   625,   626,   733,   448,
     798,   749,   637,   518,   786,   708,   293,   140,   592,   424,
     188,   247,   557,   558,   443,   562,   563,   564,   565,   566,
     567,   568,   569,   125,   314,   548,   581,   582,   583,   584,
     585,   586,   587,   322,   190,   366,   535,   526,     0,     0,
       0,   595,     0,     0,   516,   596,     0,     0,    99,     0,
       0,     0,   599,   140,   601,     0,   603,     0,     0,   605,
       0,   628,     0,   328,   329,   330,   331,   631,   332,   333,
     334,     0,   335,   336,   337,   404,     0,     0,     0,     0,
     338,   469,   470,   471,   472,   473,   474,   475,   476,   477,
     478,   479,   339,   340,   380,     0,     0,     0,   656,     0,
     342,   381,     0,     0,   658,   382,   659,     0,     0,     0,
       0,     0,   663,   383,     0,     0,   343,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   677,     0,     0,
       0,     0,     0,     0,     0,   384,     0,     0,     0,     0,
       0,     0,     0,     0,   344,     0,     0,     0,     0,     0,
       0,     0,     0,   684,     0,   685,     0,   686,     0,     0,
       0,     0,     0,     0,     0,     0,   385,   460,   461,   462,
     463,   464,   465,   466,   467,   468,   469,   470,   471,   472,
     473,   474,   475,   476,   477,   478,   479,   705,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   345,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     734,     0,     0,   737,     0,     0,   386,   468,   469,   470,
     471,   472,   473,   474,   475,   476,   477,   478,   479,     0,
       0,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,   459,   460,   461,   462,   463,   464,
     465,   466,   467,   468,   469,   470,   471,   472,   473,   474,
     475,   476,   477,   478,   479,   105,     0,     0,     0,     0,
       0,     0,     0,   507,     0,     0,     0,     0,     0,   787,
     459,   460,   461,   462,   463,   464,   465,   466,   467,   468,
     469,   470,   471,   472,   473,   474,   475,   476,   477,   478,
     479,   797,   705,     0,     0,     0,     0,     0,     0,   480,
       0,     0,     0,     0,   508,     0,     0,     0,     0,     0,
     328,   329,   330,   331,     0,   332,   333,   334,     0,   335,
     336,   337,   677,   821,     0,     0,     0,   338,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    74,     0,   339,
     340,   341,     0,     0,     0,     0,     0,   342,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   343,     0,     0,   328,   329,   330,   331,
     427,   332,   333,   334,   189,   335,   336,   337,     0,     0,
       0,     0,     0,   338,     0,     0,     0,     0,     0,     0,
       0,   344,     0,     0,     0,   339,   340,     0,     0,     0,
       0,     0,     0,   342,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   343,
       0,     0,     0,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,   345,   344,   459,   460,
     461,   462,   463,   464,   465,   466,   467,   468,   469,   470,
     471,   472,   473,   474,   475,   476,   477,   478,   479,     0,
       0,    20,     0,     0,     0,     0,     0,     0,    75,    76,
      77,    78,    79,    80,    81,    82,    83,    84,    85,    86,
      87,    88,    89,    90,    91,    92,    93,    94,    95,    96,
      97,     0,   345,     0,     0,    75,    76,    77,    78,    79,
      80,    81,    82,    83,    84,    85,    86,    87,    88,    89,
      90,    91,    92,    93,    94,    95,    96,    97,     0,     0,
       0,     0,     0,     0,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,   328,   329,   330,
     331,     0,   332,   333,   334,     0,   335,   336,   337,     0,
       0,     0,     0,     0,   338,     0,     0,     0,     0,     0,
       0,     0,   736,     0,     0,     0,   339,   340,    74,     0,
       0,     0,     0,     0,   342,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     343,     0,     0,   328,   329,   330,   331,     0,   332,   333,
     334,     0,   335,   336,   337,     0,   173,     0,     0,     0,
     338,     0,   294,     0,     0,     0,     0,     0,   344,     0,
       0,     0,   339,   340,     0,   129,     0,     0,     0,     0,
     342,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   343,     0,     0,     0,
     328,   329,   330,   331,     0,   332,   333,   334,     0,   335,
     336,   337,     1,     0,     0,     0,     0,   338,     0,     0,
       0,    20,     0,   345,   344,     0,     0,     0,   499,   339,
     340,     0,     0,     0,     0,     0,     0,   342,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   343,     0,    75,    76,    77,    78,    79,
      80,    81,    82,    83,    84,    85,    86,    87,    88,    89,
      90,    91,    92,    93,    94,    95,    96,    97,     0,   345,
       0,   344,     0,     0,     0,   501,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,    96,    97,    20,
       0,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,     0,     0,   345,     0,     0,     0,
       0,     0,     0,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,     0,     0,    75,    76,
      77,    78,    79,    80,    81,    82,    83,    84,    85,    86,
      87,    88,    89,    90,    91,    92,    93,    94,    95,    96,
      97,   328,   329,   330,   331,     0,   332,   333,   334,     0,
     335,   336,   337,     0,     0,     0,     0,     0,   338,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     339,   340,     0,     0,     0,     0,     0,     0,   342,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   343,     0,     0,     0,   328,   329,
     330,   331,     0,   332,   333,   334,     0,   335,   336,   337,
       0,     0,     0,     0,     0,   338,     0,     0,     0,     0,
       0,     0,   344,     0,     0,     0,   503,   339,   340,     0,
       0,     0,     0,     0,     0,   342,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   343,     0,     0,   328,   329,   330,   331,     0,   332,
     333,   334,     0,   335,   336,   337,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   345,     0,   344,
       0,     0,     0,   570,   571,     0,     0,     0,     0,     0,
       0,   342,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   343,     0,    75,
      76,    77,    78,    79,    80,    81,    82,    83,    84,    85,
      86,    87,    88,    89,    90,    91,    92,    93,    94,    95,
      96,    97,   129,     0,   345,   344,     0,     0,     0,     0,
       0,     0,   130,   131,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    74,     0,
       0,     0,     0,     0,     0,     0,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,    96,    97,   129,
     345,     0,     0,     0,     0,   189,     0,     0,     0,   130,
     131,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    74,     0,     0,     0,     0,     0,     0,   132,
     133,     0,    75,    76,    77,    78,    79,    80,    81,    82,
      83,    84,    85,    86,    87,    88,    89,    90,    91,    92,
      93,    94,    95,    96,    97,    74,     0,     0,     0,   189,
     134,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   135,   136,     0,
       0,     0,     0,     0,     0,     0,   132,   133,     0,    74,
       0,     0,     0,     0,     0,     0,     0,   589,     0,   294,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,   302,   135,   136,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,    96,    97,   129,
       0,     0,     0,     0,     0,     0,     0,    75,    76,    77,
      78,    79,    80,    81,    82,    83,    84,    85,    86,    87,
      88,    89,    90,    91,    92,    93,    94,    95,    96,    97,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,     0,     0,    74,     0,     0,     0,     0,
       0,     0,     0,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,   311,     0,     0,     0,
     129,     0,     0,     0,     0,     0,     0,    75,    76,    77,
      78,    79,    80,    81,    82,    83,    84,    85,    86,    87,
      88,    89,    90,    91,    92,    93,    94,    95,    96,    97,
     537,   593,    74,   459,   460,   461,   462,   463,   464,   465,
     466,   467,   468,   469,   470,   471,   472,   473,   474,   475,
     476,   477,   478,   479,     0,   105,   459,   460,   461,   462,
     463,   464,   465,   466,   467,   468,   469,   470,   471,   472,
     473,   474,   475,   476,   477,   478,   479,    75,    76,    77,
      78,    79,    80,    81,    82,    83,    84,    85,    86,    87,
      88,    89,    90,    91,    92,    93,    94,    95,    96,    97,
     129,     0,     0,   600,   459,   460,   461,   462,   463,   464,
     465,   466,   467,   468,   469,   470,   471,   472,   473,   474,
     475,   476,   477,   478,   479,     0,   602,   552,     0,     0,
       0,     0,     0,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    75,    76,
      77,    78,    79,    80,    81,    82,    83,    84,    85,    86,
      87,    88,    89,    90,    91,    92,    93,    94,    95,    96,
      97,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,   459,   460,   461,   462,
     463,   464,   465,   466,   467,   468,   469,   470,   471,   472,
     473,   474,   475,   476,   477,   478,   479,     0,    75,    76,
      77,    78,    79,    80,    81,    82,    83,    84,    85,    86,
      87,    88,    89,    90,    91,    92,    93,    94,    95,    96,
      97,   459,   460,   461,   462,   463,   464,   465,   466,   467,
     468,   469,   470,   471,   472,   473,   474,   475,   476,   477,
     478,   479,     0,     0,     0,     0,   604,     0,     0,     0,
       0,   519,   459,   460,   461,   462,   463,   464,   465,   466,
     467,   468,   469,   470,   471,   472,   473,   474,   475,   476,
     477,   478,   479,     0,     0,   629,   520,   459,   460,   461,
     462,   463,   464,   465,   466,   467,   468,   469,   470,   471,
     472,   473,   474,   475,   476,   477,   478,   479,     0,     0,
     735,   459,   460,   461,   462,   463,   464,   465,   466,   467,
     468,   469,   470,   471,   472,   473,   474,   475,   476,   477,
     478,   479,   461,   462,   463,   464,   465,   466,   467,   468,
     469,   470,   471,   472,   473,   474,   475,   476,   477,   478,
     479
};

#define yypact_value_is_default(yystate) \
  ((yystate) == (-667))

#define yytable_value_is_error(yytable_value) \
  YYID (0)

static const yytype_int16 yycheck[] =
{
       8,     9,   102,    11,    21,   342,   413,    15,   110,   164,
     165,   166,   265,    21,     8,     9,    24,   349,    15,    41,
       1,    58,   323,    41,   560,    41,    41,    10,    57,    41,
      24,    41,    28,   371,   570,   571,    41,    20,    41,    41,
      41,   110,   116,    81,     0,     1,    41,    40,    96,   110,
      81,   131,   718,    77,   392,    24,    64,    99,   110,   139,
      45,   470,    30,    20,   170,    73,     8,     9,     4,    40,
      64,   169,   244,   116,   150,   148,   248,    40,   167,    73,
     252,   253,    24,   159,    88,   161,    81,    48,    71,   189,
      75,   192,    87,   166,   102,   195,   299,   300,    15,    16,
      17,   167,    15,    16,    17,    40,   195,    42,   150,   211,
      88,    65,    73,    62,    71,   132,   140,    71,   135,   161,
      88,   195,    64,    95,   132,   115,    79,   135,    41,   195,
      66,    73,   668,   669,   670,   671,   672,   673,   674,   675,
     441,   124,   211,   172,   137,   182,   102,   195,   480,   229,
     211,   560,   195,   233,   110,   163,   194,   259,   175,   211,
     102,   570,   571,   194,   172,   831,   137,   175,   266,   163,
     124,   167,   180,   195,   137,   172,   184,   195,   157,   195,
     195,   189,   190,   195,   192,   195,   180,   195,   171,   158,
     195,   195,   195,   195,   195,   108,   368,   369,   298,   194,
     117,   209,   115,   739,   117,   195,   162,   215,   189,   158,
      57,   161,     0,   230,    40,   168,    53,   234,   235,     4,
     103,   163,   230,    20,    71,    40,   234,   235,   123,   237,
     238,   239,   240,   189,    19,    55,    41,   574,   180,   195,
     257,    41,   184,   237,   238,   239,   240,   189,   190,   257,
     192,   589,   590,   195,   421,   211,    97,    72,   425,   668,
     669,   670,   671,   672,   673,   674,   675,   209,   140,   110,
     523,   524,   525,   215,    71,   112,   609,   231,     4,     4,
      87,   361,     5,   291,    98,   365,   294,   102,    99,    12,
     298,   245,   625,   626,   302,   237,   238,   239,   240,   125,
     126,   108,    98,   259,   260,   261,   262,   144,   115,   716,
     130,     4,     5,     6,   129,     8,     9,    10,   650,    12,
      13,    14,   137,   162,    88,   172,   108,   174,   175,   176,
     739,   285,   179,   115,    43,    88,   436,    46,   438,   186,
     187,   349,   298,   190,   108,   160,   118,   188,   131,   291,
      42,   115,   294,   307,    87,   161,   298,    42,   765,    43,
     302,   342,    46,   518,   173,   195,   320,   321,    77,   178,
     150,   180,    88,     8,     3,    10,    11,   185,   395,    89,
     189,     8,    71,    10,    11,    20,   342,   395,   344,    87,
     100,   149,   150,    77,   104,   412,   185,   107,   415,   416,
     417,   108,   419,   161,   412,   191,   423,   415,   416,   417,
     418,   419,   193,   151,   395,   423,    45,    40,   426,   128,
      28,    29,    30,   133,   418,   169,    45,    46,   436,   140,
     438,   195,   426,    54,    63,    64,   260,   261,   262,   395,
     160,   458,    20,   195,   128,    64,   139,   260,   261,   262,
     458,    97,   789,    72,   273,   274,   275,    14,    77,   167,
      84,    90,   721,   722,   723,   724,   725,   726,   195,   486,
      99,   158,   480,    92,   482,    88,   418,   195,   486,    41,
     436,    40,   438,   102,   426,   174,   175,   176,    24,     4,
     179,   120,     5,    61,   436,   512,   438,   186,   187,    87,
     195,   190,    89,    24,   512,   195,   135,    62,    54,   138,
     129,    40,    84,   100,    54,   471,    89,   104,   137,   116,
     107,   150,   151,    41,   783,   119,   482,   100,   104,   116,
     632,   104,   195,   195,   107,    40,   544,     4,    40,    42,
     482,   160,   158,   116,    46,    42,   133,    49,   177,    42,
     227,   228,   181,    40,   195,    41,   815,    41,    60,   236,
     133,   143,    64,    24,   193,   242,    17,    69,   163,   195,
      72,    73,    74,   108,    47,    77,    41,   254,   195,   168,
      40,    83,   682,    85,    41,     5,   263,    40,    59,   158,
      92,   105,    40,   574,    34,    35,    36,    37,    38,    38,
     102,     4,   544,    40,    40,   561,    34,    35,    36,    37,
      38,   122,   289,    40,    40,   195,   633,    41,   574,   136,
       5,   123,    40,    93,    80,   633,   128,   129,     5,     5,
     132,     5,   134,    40,    24,   137,   653,   654,   140,   141,
     184,    10,   650,   145,    10,   653,   654,   195,    54,    24,
      38,    94,    50,    51,    52,    41,    24,    41,   160,    57,
      41,   338,   339,   340,    10,   342,   343,    65,     4,    67,
      68,   105,    70,     4,   682,     5,   632,    19,    76,     5,
     146,     5,   140,   360,   147,     8,     5,   364,    86,    40,
      32,    33,    34,    35,    36,    37,    38,     4,    41,   195,
       4,   718,    24,   101,    19,     5,   383,    24,   385,   386,
     718,     4,   389,   111,   183,   113,   114,    32,    33,    34,
      35,    36,    37,    38,     5,   561,   682,    24,    22,   127,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
     682,   162,   554,   512,   142,   653,   415,   623,   619,   781,
     427,   529,   760,   818,   168,   153,   154,   155,    32,    33,
      34,    35,    36,    37,    38,   785,   164,   165,   785,   327,
     759,   681,   522,   398,   733,   636,   189,   785,   483,   285,
     101,   151,   459,   460,   321,   462,   463,   464,   465,   466,
     467,   468,   469,    18,   210,   440,   473,   474,   475,   476,
     477,   478,   479,   215,   760,   235,   420,   412,    -1,    -1,
      -1,   488,    -1,    -1,   831,   492,    -1,    -1,   760,    -1,
      -1,    -1,   499,   831,   501,    -1,   503,    -1,    -1,   506,
      -1,   508,    -1,     3,     4,     5,     6,   514,     8,     9,
      10,    -1,    12,    13,    14,   522,    -1,    -1,    -1,    -1,
      20,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38,    32,    33,    34,    -1,    -1,    -1,   545,    -1,
      40,    41,    -1,    -1,   551,    45,   553,    -1,    -1,    -1,
      -1,    -1,   559,    53,    -1,    -1,    56,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   574,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    75,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    84,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   600,    -1,   602,    -1,   604,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   106,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,   634,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   139,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     657,    -1,    -1,   660,    -1,    -1,   156,    27,    28,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,    -1,
      -1,   171,   172,   173,   174,   175,   176,   177,   178,   179,
     180,   181,   182,   183,   184,   185,   186,   187,   188,   189,
     190,   191,   192,   193,    18,    19,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    35,    36,    37,    38,     3,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    47,    -1,    -1,    -1,    -1,    -1,   736,
      18,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,   758,   759,    -1,    -1,    -1,    -1,    -1,    -1,    47,
      -1,    -1,    -1,    -1,    88,    -1,    -1,    -1,    -1,    -1,
       3,     4,     5,     6,    -1,     8,     9,    10,    -1,    12,
      13,    14,   789,   790,    -1,    -1,    -1,    20,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,     3,    -1,    32,
      33,    34,    -1,    -1,    -1,    -1,    -1,    40,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    56,    -1,    -1,     3,     4,     5,     6,
       7,     8,     9,    10,    40,    12,    13,    14,    -1,    -1,
      -1,    -1,    -1,    20,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    84,    -1,    -1,    -1,    32,    33,    -1,    -1,    -1,
      -1,    -1,    -1,    40,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    56,
      -1,    -1,    -1,   171,   172,   173,   174,   175,   176,   177,
     178,   179,   180,   181,   182,   183,   184,   185,   186,   187,
     188,   189,   190,   191,   192,   193,   139,    84,    18,    19,
      20,    21,    22,    23,    24,    25,    26,    27,    28,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,    -1,
      -1,   137,    -1,    -1,    -1,    -1,    -1,    -1,   171,   172,
     173,   174,   175,   176,   177,   178,   179,   180,   181,   182,
     183,   184,   185,   186,   187,   188,   189,   190,   191,   192,
     193,    -1,   139,    -1,    -1,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,   185,
     186,   187,   188,   189,   190,   191,   192,   193,    -1,    -1,
      -1,    -1,    -1,    -1,   171,   172,   173,   174,   175,   176,
     177,   178,   179,   180,   181,   182,   183,   184,   185,   186,
     187,   188,   189,   190,   191,   192,   193,     3,     4,     5,
       6,    -1,     8,     9,    10,    -1,    12,    13,    14,    -1,
      -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   152,    -1,    -1,    -1,    32,    33,     3,    -1,
      -1,    -1,    -1,    -1,    40,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      56,    -1,    -1,     3,     4,     5,     6,    -1,     8,     9,
      10,    -1,    12,    13,    14,    -1,    41,    -1,    -1,    -1,
      20,    -1,    47,    -1,    -1,    -1,    -1,    -1,    84,    -1,
      -1,    -1,    32,    33,    -1,     3,    -1,    -1,    -1,    -1,
      40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    56,    -1,    -1,    -1,
       3,     4,     5,     6,    -1,     8,     9,    10,    -1,    12,
      13,    14,    40,    -1,    -1,    -1,    -1,    20,    -1,    -1,
      -1,   137,    -1,   139,    84,    -1,    -1,    -1,    88,    32,
      33,    -1,    -1,    -1,    -1,    -1,    -1,    40,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    56,    -1,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,   185,
     186,   187,   188,   189,   190,   191,   192,   193,    -1,   139,
      -1,    84,    -1,    -1,    -1,    88,   171,   172,   173,   174,
     175,   176,   177,   178,   179,   180,   181,   182,   183,   184,
     185,   186,   187,   188,   189,   190,   191,   192,   193,   137,
      -1,   171,   172,   173,   174,   175,   176,   177,   178,   179,
     180,   181,   182,   183,   184,   185,   186,   187,   188,   189,
     190,   191,   192,   193,    -1,    -1,   139,    -1,    -1,    -1,
      -1,    -1,    -1,   171,   172,   173,   174,   175,   176,   177,
     178,   179,   180,   181,   182,   183,   184,   185,   186,   187,
     188,   189,   190,   191,   192,   193,    -1,    -1,   171,   172,
     173,   174,   175,   176,   177,   178,   179,   180,   181,   182,
     183,   184,   185,   186,   187,   188,   189,   190,   191,   192,
     193,     3,     4,     5,     6,    -1,     8,     9,    10,    -1,
      12,    13,    14,    -1,    -1,    -1,    -1,    -1,    20,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      32,    33,    -1,    -1,    -1,    -1,    -1,    -1,    40,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    56,    -1,    -1,    -1,     3,     4,
       5,     6,    -1,     8,     9,    10,    -1,    12,    13,    14,
      -1,    -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,    -1,
      -1,    -1,    84,    -1,    -1,    -1,    88,    32,    33,    -1,
      -1,    -1,    -1,    -1,    -1,    40,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    56,    -1,    -1,     3,     4,     5,     6,    -1,     8,
       9,    10,    -1,    12,    13,    14,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   139,    -1,    84,
      -1,    -1,    -1,    32,    33,    -1,    -1,    -1,    -1,    -1,
      -1,    40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    56,    -1,   171,
     172,   173,   174,   175,   176,   177,   178,   179,   180,   181,
     182,   183,   184,   185,   186,   187,   188,   189,   190,   191,
     192,   193,     3,    -1,   139,    84,    -1,    -1,    -1,    -1,
      -1,    -1,    13,    14,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,     3,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   171,   172,   173,   174,
     175,   176,   177,   178,   179,   180,   181,   182,   183,   184,
     185,   186,   187,   188,   189,   190,   191,   192,   193,     3,
     139,    -1,    -1,    -1,    -1,    40,    -1,    -1,    -1,    13,
      14,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,     3,    -1,    -1,    -1,    -1,    -1,    -1,    90,
      91,    -1,   171,   172,   173,   174,   175,   176,   177,   178,
     179,   180,   181,   182,   183,   184,   185,   186,   187,   188,
     189,   190,   191,   192,   193,     3,    -1,    -1,    -1,    40,
     121,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   138,   139,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    90,    91,    -1,     3,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,    47,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,   185,   186,   187,   188,   189,   190,
     191,   192,   193,    47,   138,   139,   171,   172,   173,   174,
     175,   176,   177,   178,   179,   180,   181,   182,   183,   184,
     185,   186,   187,   188,   189,   190,   191,   192,   193,     3,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   171,   172,   173,
     174,   175,   176,   177,   178,   179,   180,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,   185,   186,   187,   188,   189,   190,
     191,   192,   193,    -1,    -1,     3,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   171,   172,   173,   174,   175,   176,   177,
     178,   179,   180,   181,   182,   183,   184,   185,   186,   187,
     188,   189,   190,   191,   192,   193,    34,    -1,    -1,    -1,
       3,    -1,    -1,    -1,    -1,    -1,    -1,   171,   172,   173,
     174,   175,   176,   177,   178,   179,   180,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193,
     124,    34,     3,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    -1,     3,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,   171,   172,   173,
     174,   175,   176,   177,   178,   179,   180,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193,
       3,    -1,    -1,    88,    18,    19,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    35,    36,    37,    38,    -1,    88,    41,    -1,    -1,
      -1,    -1,    -1,   171,   172,   173,   174,   175,   176,   177,
     178,   179,   180,   181,   182,   183,   184,   185,   186,   187,
     188,   189,   190,   191,   192,   193,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   171,   172,
     173,   174,   175,   176,   177,   178,   179,   180,   181,   182,
     183,   184,   185,   186,   187,   188,   189,   190,   191,   192,
     193,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,   185,   186,   187,   188,   189,   190,
     191,   192,   193,   171,   172,   173,   174,   175,   176,   177,
     178,   179,   180,   181,   182,   183,   184,   185,   186,   187,
     188,   189,   190,   191,   192,   193,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    -1,   171,   172,
     173,   174,   175,   176,   177,   178,   179,   180,   181,   182,
     183,   184,   185,   186,   187,   188,   189,   190,   191,   192,
     193,    18,    19,    20,    21,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38,    -1,    -1,    -1,    -1,    88,    -1,    -1,    -1,
      -1,    48,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    -1,    -1,    41,    73,    18,    19,    20,
      21,    22,    23,    24,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,    -1,    -1,
      41,    18,    19,    20,    21,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,    40,    46,    49,    60,    64,    69,    72,    73,    74,
      77,    83,    85,    92,   102,   123,   128,   129,   132,   134,
     137,   140,   141,   145,   160,   197,   198,   199,   214,   215,
     218,   235,   241,   244,   245,   246,   251,   252,   253,   254,
     255,   256,   283,   286,   292,   297,   299,   301,   304,   308,
     309,   310,   311,   316,   318,   321,   326,   329,   330,   331,
     337,   252,   253,   149,   150,   161,   170,   306,   306,   150,
     159,   161,   236,    88,     3,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,   185,
     186,   187,   188,   189,   190,   191,   192,   193,   279,   346,
     349,   279,    99,   150,   161,     3,   319,   348,   349,   192,
     285,    45,    46,    64,    72,    77,    92,   102,   129,   137,
     160,   312,   313,   319,   161,   312,   306,    95,   259,     3,
      13,    14,    90,    91,   121,   138,   139,   322,   323,   345,
     349,     3,    45,    63,    64,    90,    99,   120,   135,   138,
     150,   177,   181,   193,   212,   289,   341,   347,   157,   279,
       0,    81,   194,   103,    15,    16,    17,   117,   268,   269,
      87,   265,   123,    41,    41,   140,   279,     4,   295,    98,
     219,   293,   294,   295,    99,   279,     4,   291,   291,    40,
     252,   278,   279,   280,    98,   242,   295,   298,   162,   327,
     214,   215,   245,   251,   284,   131,   314,   161,   118,   116,
     195,    88,   295,   302,   303,   116,   317,    40,   125,   126,
     260,   261,    45,    75,   274,    24,   158,   324,   324,   345,
      42,    87,   300,   345,    42,   195,   324,    30,    88,   150,
      88,    28,   167,   290,   185,    87,   288,   300,   185,   108,
     287,   193,   191,   151,    40,   169,   307,   140,   199,   279,
     274,   274,   274,    54,   108,   115,   258,   265,   160,   319,
     338,   339,   345,    43,    46,    77,   128,   332,   333,    97,
     110,   188,   305,    20,   279,   195,    97,   237,   346,   167,
     257,   252,   278,   280,    47,   346,    89,   100,   104,   107,
     133,   281,    47,   346,    84,   243,   278,   195,    14,   325,
     328,    34,   315,   346,   313,   214,   215,   245,   251,   320,
     158,   195,   315,    88,    41,    40,    96,   195,     3,     4,
       5,     6,     8,     9,    10,    12,    13,    14,    20,    32,
      33,    34,    40,    56,    84,   139,   201,   202,   203,   205,
     207,   212,   252,   275,   276,   345,   346,   349,   205,   205,
     324,   345,   295,    24,   324,   345,   323,   205,   279,   279,
     279,   279,     4,   205,   290,    66,   290,     5,   290,   290,
      34,    41,    45,    53,    75,   106,   156,   200,   205,   213,
     266,    61,   216,   217,   345,    40,   247,   251,   137,   252,
     255,   256,   255,   255,   205,   270,   271,     5,    12,   263,
     263,   265,   195,    24,    62,   334,   334,   334,   158,   334,
     195,    54,    84,    40,   294,    54,   116,     7,   205,    41,
     346,   119,   282,   278,   282,   282,   104,   346,   195,   295,
     195,   158,   295,   302,   298,   148,   166,   262,   261,   205,
     205,   205,   200,   205,   252,   205,   208,   252,    42,    18,
      19,    20,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      47,   348,    88,   195,   258,   264,    42,   205,   324,     4,
     296,   205,   324,   290,   290,   257,    40,   195,    41,    88,
     205,    88,   205,    88,   205,    41,   195,    47,    88,   205,
      41,   143,   195,   257,    24,   248,   345,   163,   259,    48,
      73,   272,   195,   115,   195,   108,   339,   202,   222,   345,
     345,   345,   279,   345,   128,   333,   296,   124,   220,   221,
     222,   296,   279,   205,    47,   116,   278,   278,   325,   298,
      41,   195,    41,   168,   209,   210,   345,   205,   205,    28,
      29,    30,   205,   205,   205,   205,   205,   205,   205,   205,
      32,    33,   203,   204,    40,   206,   252,     8,    10,    11,
      20,   205,   205,   205,   205,   205,   205,   205,   348,    78,
     277,   278,   275,    34,   345,   205,   205,    41,     5,   205,
      88,   205,    88,   205,    88,   205,    50,    51,    52,    57,
      65,    67,    68,    70,    76,    86,   101,   111,   113,   114,
     127,   142,   153,   154,   155,   164,   165,   223,   205,    41,
     217,   205,    41,   195,    40,   249,   274,   271,   263,   263,
     263,    59,   340,   223,    77,   140,   336,    55,   130,   335,
     158,   105,    41,   195,    40,   346,   205,   116,   205,   205,
      79,   210,   211,   205,   204,   206,   204,   204,    19,    32,
      33,    34,    35,    36,    37,    38,   200,   205,     8,    10,
      11,   257,   195,   257,   205,   205,   205,    40,   228,   228,
      40,   224,   122,   226,    40,   225,   224,    40,   227,   227,
     228,   228,    41,   251,   345,   205,   250,   195,   276,     4,
     136,   342,   229,    20,    71,    20,    71,   348,    40,    71,
     174,   175,   176,   179,   186,   187,   190,   231,   232,   234,
     221,   238,   239,   345,   205,    41,   152,   205,    80,    19,
     204,   204,   204,   204,   204,   204,   204,   204,    41,   264,
     278,    93,   267,     5,     5,     5,     5,    41,   195,    40,
      88,    24,   184,    10,    20,    71,   124,   171,   230,    10,
      10,   202,   248,    24,   233,   233,   233,   233,   233,   233,
     233,   195,    57,   172,    41,   195,   272,   205,   204,    54,
      94,   273,    41,    41,   195,    41,    41,   205,   250,    53,
     112,   144,    24,    10,   202,   105,    41,     4,   146,     4,
       5,     5,     5,     8,   232,   140,   233,   147,   240,   239,
     200,   205,     5,    41,   173,   178,   180,   189,   343,   233,
       4,    40,   231,    41,    58,   182,   344,     4,   248,    24,
      24,    41,     5,     4,   183,    24,     5
};

#define yyerrok		(yyerrstatus = 0)
#define yyclearin	(yychar = YYEMPTY)
#define YYEMPTY		(-2)
#define YYEOF		0

#define YYACCEPT	goto yyacceptlab
#define YYABORT		goto yyabortlab
#define YYERROR		goto yyerrorlab


/* Like YYERROR except do call yyerror.  This remains here temporarily
   to ease the transition to the new meaning of YYERROR, for GCC.
   Once GCC version 2 has supplanted version 1, this can go.  However,
   YYFAIL appears to be in use.  Nevertheless, it is formally deprecated
   in Bison 2.4.2's NEWS entry, where a plan to phase it out is
   discussed.  */

#define YYFAIL		goto yyerrlab
#if defined YYFAIL
  /* This is here to suppress warnings from the GCC cpp's
     -Wunused-macros.  Normally we don't worry about that warning, but
     some users do, and we want to make it easy for users to remove
     YYFAIL uses, which will produce warnings from Bison 2.5.  */
#endif

#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)					\
do								\
  if (yychar == YYEMPTY && yylen == 1)				\
    {								\
      yychar = (Token);						\
      yylval = (Value);						\
      YYPOPSTACK (1);						\
      goto yybackup;						\
    }								\
  else								\
    {								\
      yyerror (&yylloc, result, YY_("syntax error: cannot back up")); \
      YYERROR;							\
    }								\
while (YYID (0))


#define YYTERROR	1
#define YYERRCODE	256


/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#define YYRHSLOC(Rhs, K) ((Rhs)[K])
#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)				\
    do									\
      if (YYID (N))                                                    \
	{								\
	  (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;	\
	  (Current).first_column = YYRHSLOC (Rhs, 1).first_column;	\
	  (Current).last_line    = YYRHSLOC (Rhs, N).last_line;		\
	  (Current).last_column  = YYRHSLOC (Rhs, N).last_column;	\
	}								\
      else								\
	{								\
	  (Current).first_line   = (Current).last_line   =		\
	    YYRHSLOC (Rhs, 0).last_line;				\
	  (Current).first_column = (Current).last_column =		\
	    YYRHSLOC (Rhs, 0).last_column;				\
	}								\
    while (YYID (0))
#endif


/* YY_LOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

#ifndef YY_LOCATION_PRINT
# if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
#  define YY_LOCATION_PRINT(File, Loc)			\
     fprintf (File, "%d.%d-%d.%d",			\
	      (Loc).first_line, (Loc).first_column,	\
	      (Loc).last_line,  (Loc).last_column)
# else
#  define YY_LOCATION_PRINT(File, Loc) ((void) 0)
# endif
#endif


/* YYLEX -- calling `yylex' with the right arguments.  */

#ifdef YYLEX_PARAM
# define YYLEX yylex (&yylval, &yylloc, YYLEX_PARAM)
#else
# define YYLEX yylex (&yylval, &yylloc)
#endif

/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)			\
do {						\
  if (yydebug)					\
    YYFPRINTF Args;				\
} while (YYID (0))

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)			  \
do {									  \
  if (yydebug)								  \
    {									  \
      YYFPRINTF (stderr, "%s ", Title);					  \
      yy_symbol_print (stderr,						  \
		  Type, Value, Location, result); \
      YYFPRINTF (stderr, "\n");						  \
    }									  \
} while (YYID (0))


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ParseResult* result)
#else
static void
yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ParseResult* result;
#endif
{
  if (!yyvaluep)
    return;
  YYUSE (yylocationp);
  YYUSE (result);
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# else
  YYUSE (yyoutput);
# endif
  switch (yytype)
    {
      default:
	break;
    }
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ParseResult* result)
#else
static void
yy_symbol_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ParseResult* result;
#endif
{
  if (yytype < YYNTOKENS)
    YYFPRINTF (yyoutput, "token %s (", yytname[yytype]);
  else
    YYFPRINTF (yyoutput, "nterm %s (", yytname[yytype]);

  YY_LOCATION_PRINT (yyoutput, *yylocationp);
  YYFPRINTF (yyoutput, ": ");
  yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
#else
static void
yy_stack_print (yybottom, yytop)
    yytype_int16 *yybottom;
    yytype_int16 *yytop;
#endif
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)				\
do {								\
  if (yydebug)							\
    yy_stack_print ((Bottom), (Top));				\
} while (YYID (0))


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_reduce_print (YYSTYPE *yyvsp, YYLTYPE *yylsp, int yyrule, ParseResult* result)
#else
static void
yy_reduce_print (yyvsp, yylsp, yyrule, result)
    YYSTYPE *yyvsp;
    YYLTYPE *yylsp;
    int yyrule;
    ParseResult* result;
#endif
{
  int yynrhs = yyr2[yyrule];
  int yyi;
  unsigned long int yylno = yyrline[yyrule];
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
	     yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr, yyrhs[yyprhs[yyrule] + yyi],
		       &(yyvsp[(yyi + 1) - (yynrhs)])
		       , &(yylsp[(yyi + 1) - (yynrhs)])		       , result);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)		\
do {					\
  if (yydebug)				\
    yy_reduce_print (yyvsp, yylsp, Rule, result); \
} while (YYID (0))

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef	YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static YYSIZE_T
yystrlen (const char *yystr)
#else
static YYSIZE_T
yystrlen (yystr)
    const char *yystr;
#endif
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static char *
yystpcpy (char *yydest, const char *yysrc)
#else
static char *
yystpcpy (yydest, yysrc)
    char *yydest;
    const char *yysrc;
#endif
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
	switch (*++yyp)
	  {
	  case '\'':
	  case ',':
	    goto do_not_strip_quotes;

	  case '\\':
	    if (*++yyp != '\\')
	      goto do_not_strip_quotes;
	    /* Fall through.  */
	  default:
	    if (yyres)
	      yyres[yyn] = *yyp;
	    yyn++;
	    break;

	  case '"':
	    if (yyres)
	      yyres[yyn] = '\0';
	    return yyn;
	  }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return 1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return 2 if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYSIZE_T *yymsg_alloc, char **yymsg,
                yytype_int16 *yyssp, int yytoken)
{
  YYSIZE_T yysize0 = yytnamerr (0, yytname[yytoken]);
  YYSIZE_T yysize = yysize0;
  YYSIZE_T yysize1;
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = 0;
  /* Arguments of yyformat. */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Number of reported tokens (one for the "unexpected", one per
     "expected"). */
  int yycount = 0;

  /* There are many possibilities here to consider:
     - Assume YYFAIL is not used.  It's too flawed to consider.  See
       <http://lists.gnu.org/archive/html/bison-patches/2009-12/msg00024.html>
       for details.  YYERROR is fine as it does not invoke this
       function.
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yytoken != YYEMPTY)
    {
      int yyn = yypact[*yyssp];
      yyarg[yycount++] = yytname[yytoken];
      if (!yypact_value_is_default (yyn))
        {
          /* Start YYX at -YYN if negative to avoid negative indexes in
             YYCHECK.  In other words, skip the first -YYN actions for
             this state because they are default actions.  */
          int yyxbegin = yyn < 0 ? -yyn : 0;
          /* Stay within bounds of both yycheck and yytname.  */
          int yychecklim = YYLAST - yyn + 1;
          int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
          int yyx;

          for (yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR
                && !yytable_value_is_error (yytable[yyx + yyn]))
              {
                if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                  {
                    yycount = 1;
                    yysize = yysize0;
                    break;
                  }
                yyarg[yycount++] = yytname[yyx];
                yysize1 = yysize + yytnamerr (0, yytname[yyx]);
                if (! (yysize <= yysize1
                       && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
                  return 2;
                yysize = yysize1;
              }
        }
    }

  switch (yycount)
    {
# define YYCASE_(N, S)                      \
      case N:                               \
        yyformat = S;                       \
      break
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
# undef YYCASE_
    }

  yysize1 = yysize + yystrlen (yyformat);
  if (! (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
    return 2;
  yysize = yysize1;

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return 1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yyarg[yyi++]);
          yyformat += 2;
        }
      else
        {
          yyp++;
          yyformat++;
        }
  }
  return 0;
}
#endif /* YYERROR_VERBOSE */

/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, ParseResult* result)
#else
static void
yydestruct (yymsg, yytype, yyvaluep, yylocationp, result)
    const char *yymsg;
    int yytype;
    YYSTYPE *yyvaluep;
    YYLTYPE *yylocationp;
    ParseResult* result;
#endif
{
  YYUSE (yyvaluep);
  YYUSE (yylocationp);
  YYUSE (result);

  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  switch (yytype)
    {
      case 3: /* "NAME" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 4: /* "STRING" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 5: /* "INTNUM" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 6: /* "DATE_VALUE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 7: /* "HINT_VALUE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 8: /* "BOOL" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 9: /* "APPROXNUM" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 10: /* "NULLX" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 11: /* "UNKNOWN" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 12: /* "QUESTIONMARK" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 13: /* "SYSTEM_VARIABLE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 14: /* "TEMP_VARIABLE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 197: /* "sql_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 198: /* "stmt_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 199: /* "stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 200: /* "expr_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 201: /* "column_ref" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 202: /* "expr_const" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 203: /* "simple_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 204: /* "arith_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 205: /* "expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 206: /* "in_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 207: /* "case_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 208: /* "case_arg" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 209: /* "when_clause_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 210: /* "when_clause" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 211: /* "case_default" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 212: /* "func_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 213: /* "distinct_or_all" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 214: /* "delete_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 215: /* "update_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 216: /* "update_asgn_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 217: /* "update_asgn_factor" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 218: /* "create_table_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 219: /* "opt_if_not_exists" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 220: /* "table_element_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 221: /* "table_element" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 222: /* "column_definition" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 223: /* "data_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 224: /* "opt_decimal" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 225: /* "opt_float" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 226: /* "opt_precision" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 227: /* "opt_time_precision" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 228: /* "opt_char_length" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 229: /* "opt_column_attribute_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 230: /* "column_attribute" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 231: /* "opt_table_option_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 232: /* "table_option" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 233: /* "opt_equal_mark" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 234: /* "opt_default_mark" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 235: /* "create_index_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 237: /* "index_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 238: /* "sort_column_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 239: /* "sort_column_key" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 240: /* "opt_storing" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 241: /* "drop_table_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 242: /* "opt_if_exists" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 243: /* "table_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 244: /* "drop_index_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 245: /* "insert_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 246: /* "replace_or_insert" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 247: /* "opt_insert_columns" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 248: /* "column_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 249: /* "insert_vals_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 250: /* "insert_vals" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 251: /* "select_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 252: /* "select_with_parens" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 253: /* "select_no_parens" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 254: /* "no_table_select" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 255: /* "select_clause" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 256: /* "simple_select" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 257: /* "opt_where" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 258: /* "select_limit" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 259: /* "opt_hint" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 260: /* "opt_hint_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 261: /* "hint_option" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 263: /* "limit_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 264: /* "opt_select_limit" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 265: /* "opt_for_update" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 266: /* "parameterized_trim" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 267: /* "opt_groupby" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 268: /* "opt_order_by" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 269: /* "order_by" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 270: /* "sort_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 271: /* "sort_key" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 272: /* "opt_asc_desc" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 273: /* "opt_having" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 274: /* "opt_distinct" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 275: /* "projection" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 276: /* "select_expr_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 277: /* "from_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 278: /* "table_factor" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 279: /* "relation_factor" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 280: /* "joined_table" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 281: /* "join_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 282: /* "join_outer" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 283: /* "explain_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 284: /* "explainable_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 285: /* "opt_verbose" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 286: /* "show_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 287: /* "opt_limit" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 288: /* "opt_for_grant_user" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 290: /* "opt_show_condition" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 291: /* "opt_like_condition" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 292: /* "create_user_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 293: /* "user_specification_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 294: /* "user_specification" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 295: /* "user" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 296: /* "password" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 297: /* "drop_user_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 298: /* "user_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 299: /* "set_password_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 300: /* "opt_for_user" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 301: /* "rename_user_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 302: /* "rename_info" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 303: /* "rename_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 304: /* "lock_user_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 305: /* "lock_spec" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 306: /* "opt_work" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 308: /* "begin_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 309: /* "commit_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 310: /* "rollback_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 311: /* "grant_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 312: /* "priv_type_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 313: /* "priv_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 314: /* "opt_privilege" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 315: /* "priv_level" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 316: /* "revoke_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 317: /* "opt_on_priv_level" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 318: /* "prepare_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 319: /* "stmt_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 320: /* "preparable_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 321: /* "variable_set_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 322: /* "var_and_val_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 323: /* "var_and_val" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 324: /* "to_or_eq" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 325: /* "argument" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 326: /* "execute_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 327: /* "opt_using_args" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 328: /* "argument_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 329: /* "deallocate_prepare_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 330: /* "deallocate_or_drop" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 331: /* "alter_table_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 332: /* "alter_column_actions" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 333: /* "alter_column_action" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 334: /* "opt_column" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 336: /* "alter_column_behavior" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 337: /* "alter_system_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 338: /* "alter_system_actions" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 339: /* "alter_system_action" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 340: /* "opt_comment" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 343: /* "server_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 344: /* "opt_cluster_or_address" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 345: /* "column_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 346: /* "relation_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 347: /* "function_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 348: /* "column_label" */

	{destroy_tree((yyvaluep->node));};

	break;

      default:
	break;
    }
}


/* Prevent warnings from -Wmissing-prototypes.  */
#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (ParseResult* result);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */


/*----------.
| yyparse.  |
`----------*/

#ifdef YYPARSE_PARAM
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void *YYPARSE_PARAM)
#else
int
yyparse (YYPARSE_PARAM)
    void *YYPARSE_PARAM;
#endif
#else /* ! YYPARSE_PARAM */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (ParseResult* result)
#else
int
yyparse (result)
    ParseResult* result;
#endif
#endif
{
/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;

/* Location data for the lookahead symbol.  */
YYLTYPE yylloc;

    /* Number of syntax errors so far.  */
    int yynerrs;

    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       `yyss': related to states.
       `yyvs': related to semantic values.
       `yyls': related to locations.

       Refer to the stacks thru separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    /* The location stack.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls;
    YYLTYPE *yylsp;

    /* The locations where the error started and ended.  */
    YYLTYPE yyerror_range[3];

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yytoken = 0;
  yyss = yyssa;
  yyvs = yyvsa;
  yyls = yylsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */

  /* Initialize stack pointers.
     Waste one element of value and location stack
     so that they stay on the same level as the state stack.
     The wasted elements are never initialized.  */
  yyssp = yyss;
  yyvsp = yyvs;
  yylsp = yyls;

#if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
  /* Initialize the default location before parsing starts.  */
  yylloc.first_line   = yylloc.last_line   = 1;
  yylloc.first_column = yylloc.last_column = 1;
#endif

  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
	/* Give user a chance to reallocate the stack.  Use copies of
	   these so that the &'s don't force the real ones into
	   memory.  */
	YYSTYPE *yyvs1 = yyvs;
	yytype_int16 *yyss1 = yyss;
	YYLTYPE *yyls1 = yyls;

	/* Each stack pointer address is followed by the size of the
	   data in use in that stack, in bytes.  This used to be a
	   conditional around just the two extra args, but that might
	   be undefined if yyoverflow is a macro.  */
	yyoverflow (YY_("memory exhausted"),
		    &yyss1, yysize * sizeof (*yyssp),
		    &yyvs1, yysize * sizeof (*yyvsp),
		    &yyls1, yysize * sizeof (*yylsp),
		    &yystacksize);

	yyls = yyls1;
	yyss = yyss1;
	yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
	goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
	yystacksize = YYMAXDEPTH;

      {
	yytype_int16 *yyss1 = yyss;
	union yyalloc *yyptr =
	  (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
	if (! yyptr)
	  goto yyexhaustedlab;
	YYSTACK_RELOCATE (yyss_alloc, yyss);
	YYSTACK_RELOCATE (yyvs_alloc, yyvs);
	YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
	if (yyss1 != yyssa)
	  YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
		  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
	YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = YYLEX;
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  *++yyvsp = yylval;
  *++yylsp = yylloc;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     `$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location.  */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 2:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_STMT_LIST, (yyvsp[(1) - (2)].node));
      result->result_tree_ = (yyval.node);
      YYACCEPT;
    }
    break;

  case 3:

    {
      if ((yyvsp[(3) - (3)].node) != NULL)
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      else
        (yyval.node) = (yyvsp[(1) - (3)].node);
    }
    break;

  case 4:

    {
      (yyval.node) = ((yyvsp[(1) - (1)].node) != NULL) ? (yyvsp[(1) - (1)].node) : NULL;
    }
    break;

  case 5:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 6:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 7:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 8:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 9:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 10:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 11:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 12:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 13:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 14:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 15:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 16:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 17:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 18:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 19:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 20:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 21:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 22:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 23:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 24:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 25:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 26:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 27:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 28:

    {(yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 31:

    { (yyval.node) = NULL; }
    break;

  case 32:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 33:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 34:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 35:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NAME_FIELD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(3) - (3)]).first_column, (yylsp[(3) - (3)]).last_column);
    }
    break;

  case 36:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NAME_FIELD, 2, (yyvsp[(1) - (3)].node), node);
    }
    break;

  case 37:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 38:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 39:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 40:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 41:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 42:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 43:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 44:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 45:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 46:

    { (yyvsp[(3) - (3)].node)->type_ = T_SYSTEM_VARIABLE; (yyval.node) = (yyvsp[(3) - (3)].node); }
    break;

  case 47:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 48:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 49:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 50:

    {
      ParseNode *node = NULL;
      malloc_non_terminal_node(node, result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(2) - (5)].node), (yyvsp[(4) - (5)].node));
      merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, node);
    }
    break;

  case 51:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      /*
      yyerror(&@1, result, "CASE expression is not supported yet!");
      YYABORT;
      */
    }
    break;

  case 52:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 53:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 54:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_EXISTS, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 55:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 56:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POS, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 57:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NEG, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 58:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_ADD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 59:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MINUS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 60:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 61:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_DIV, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 62:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_REM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 63:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POW, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 64:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MOD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 65:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 66:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POS, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 67:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NEG, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 68:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_ADD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 69:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MINUS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 70:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 71:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_DIV, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 72:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_REM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 73:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POW, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 74:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MOD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 75:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 76:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LT, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 77:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_EQ, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 78:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_GE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 79:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_GT, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 80:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 81:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 82:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_LIKE, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node)); }
    break;

  case 83:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_AND, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 84:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_OR, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 85:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 86:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 87:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 88:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 89:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 90:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 91:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 92:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_BTW, 3, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 93:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_BTW, 3, (yyvsp[(1) - (6)].node), (yyvsp[(4) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 94:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IN, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 95:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_IN, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 96:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_CNN, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 97:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 98:

    { merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, (yyvsp[(2) - (3)].node)); }
    break;

  case 99:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_WHEN_LIST, (yyvsp[(3) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CASE, 3, (yyvsp[(2) - (5)].node), (yyval.node), (yyvsp[(4) - (5)].node));
    }
    break;

  case 100:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 101:

    { (yyval.node) = NULL; }
    break;

  case 102:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 103:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); }
    break;

  case 104:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHEN, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 105:

    { (yyval.node) = (yyvsp[(2) - (2)].node); }
    break;

  case 106:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_NULL); }
    break;

  case 107:

    {
      if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "count") != 0)
      {
        yyerror(&(yylsp[(1) - (4)]), result, "Only COUNT function can be with '*' parameter!");
        YYABORT;
      }
      else
      {
        ParseNode* node = NULL;
        malloc_terminal_node(node, result->malloc_pool_, T_STAR);
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 1, node);
      }
    }
    break;

  case 108:

    {
      if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "count") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "sum") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SUM, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "max") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MAX, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "min") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MIN, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "avg") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_AVG, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else
      {
        yyerror(&(yylsp[(1) - (5)]), result, "Wrong system function with 'DISTINCT/ALL'!");
        YYABORT;
      }
    }
    break;

  case 109:

    {
      if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "count") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "COUNT function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "sum") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "SUM function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SUM, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "max") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "MAX function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MAX, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "min") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "MIN function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MIN, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "avg") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "AVG function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_AVG, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "trim") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "TRIM function syntax error! TRIM don't take %d params", (yyvsp[(3) - (4)].node)->num_child_);
          YYABORT;
        }
        else
        {
          ParseNode* default_type = NULL;
          malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
          default_type->value_ = 0;
          ParseNode* default_operand = NULL;
          malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
          default_operand->str_value_ = " "; /* blank for default */
          default_operand->value_ = strlen(default_operand->str_value_);
          ParseNode *params = NULL;
          malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (4)].node));
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), params);
        }
      }
      else  /* system function */
      {
        ParseNode *params = NULL;
        merge_nodes(params, result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (4)].node));
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), params);
      }
    }
    break;

  case 110:

    {
      if (strcasecmp((yyvsp[(1) - (6)].node)->str_value_, "cast") == 0)
      {
        (yyvsp[(5) - (6)].node)->value_ = (yyvsp[(5) - (6)].node)->type_;
        (yyvsp[(5) - (6)].node)->type_ = T_INT;
        ParseNode *params = NULL;
        malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, (yyvsp[(3) - (6)].node), (yyvsp[(5) - (6)].node));
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (6)].node), params);
      }
      else
      {
        yyerror(&(yylsp[(1) - (6)]), result, "AS support cast function only!");
        YYABORT;
      }
    }
    break;

  case 111:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), (yyvsp[(3) - (4)].node));
    }
    break;

  case 112:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 1, (yyvsp[(1) - (3)].node));
      //yyerror(&@1, result, "system/user-define function is not supported yet!");
      //YYABORT;
    }
    break;

  case 113:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ALL);
    }
    break;

  case 114:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_DISTINCT);
    }
    break;

  case 115:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DELETE, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 116:

    {
      ParseNode* assign_list = NULL;
      merge_nodes(assign_list, result->malloc_pool_, T_ASSIGN_LIST, (yyvsp[(4) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_UPDATE, 3, (yyvsp[(2) - (5)].node), assign_list, (yyvsp[(5) - (5)].node));
    }
    break;

  case 117:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 118:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 119:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ASSIGN_ITEM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 120:

    {
      ParseNode *table_elements = NULL;
      ParseNode *table_options = NULL;
      merge_nodes(table_elements, result->malloc_pool_, T_TABLE_ELEMENT_LIST, (yyvsp[(6) - (8)].node));
      merge_nodes(table_options, result->malloc_pool_, T_TABLE_OPTION_LIST, (yyvsp[(8) - (8)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_TABLE, 4,
              (yyvsp[(3) - (8)].node),                   /* if not exists */
              (yyvsp[(4) - (8)].node),                   /* table name */
              table_elements,       /* columns or primary key */
              table_options         /* table option(s) */
              );
    }
    break;

  case 121:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_IF_NOT_EXISTS); }
    break;

  case 122:

    { (yyval.node) = NULL; }
    break;

  case 123:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 124:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 125:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 126:

    {
      ParseNode* col_list= NULL;
      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(4) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PRIMARY_KEY, 1, col_list);
    }
    break;

  case 127:

    {
      ParseNode *attributes = NULL;
      merge_nodes(attributes, result->malloc_pool_, T_COLUMN_ATTRIBUTES, (yyvsp[(3) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_DEFINITION, 3, (yyvsp[(1) - (3)].node), (yyvsp[(2) - (3)].node), attributes);
    }
    break;

  case 128:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER ); }
    break;

  case 129:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); }
    break;

  case 130:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); }
    break;

  case 131:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); }
    break;

  case 132:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); }
    break;

  case 133:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL);
      else
        merge_nodes((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL, (yyvsp[(2) - (2)].node));
      yyerror(&(yylsp[(1) - (2)]), result, "DECIMAL type is not supported");
      YYABORT;
    }
    break;

  case 134:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL);
      else
        merge_nodes((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL, (yyvsp[(2) - (2)].node));
      yyerror(&(yylsp[(1) - (2)]), result, "NUMERIC type is not supported");
      YYABORT;
    }
    break;

  case 135:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_BOOLEAN ); }
    break;

  case 136:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_FLOAT, 1, (yyvsp[(2) - (2)].node)); }
    break;

  case 137:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DOUBLE); }
    break;

  case 138:

    {
      (void)((yyvsp[(2) - (2)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DOUBLE);
    }
    break;

  case 139:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 140:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP); }
    break;

  case 141:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 142:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 143:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 144:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 145:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CREATETIME); }
    break;

  case 146:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_MODIFYTIME); }
    break;

  case 147:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DATE);
      yyerror(&(yylsp[(1) - (1)]), result, "DATE type is not supported");
      YYABORT;
    }
    break;

  case 148:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIME);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIME, 1, (yyvsp[(2) - (2)].node));
      yyerror(&(yylsp[(1) - (2)]), result, "TIME type is not supported");
      YYABORT;
    }
    break;

  case 149:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(2) - (5)].node), (yyvsp[(4) - (5)].node)); }
    break;

  case 150:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 151:

    { (yyval.node) = NULL; }
    break;

  case 152:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 153:

    { (yyval.node) = NULL; }
    break;

  case 154:

    { (yyval.node) = NULL; }
    break;

  case 155:

    { (yyval.node) = NULL; }
    break;

  case 156:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 157:

    { (yyval.node) = NULL; }
    break;

  case 158:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 159:

    { (yyval.node) = NULL; }
    break;

  case 160:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); }
    break;

  case 161:

    { (yyval.node) = NULL; }
    break;

  case 162:

    {
      (void)((yyvsp[(2) - (2)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NOT_NULL);
    }
    break;

  case 163:

    {
      (void)((yyvsp[(1) - (1)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NULL);
    }
    break;

  case 164:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyvsp[(2) - (2)].node)); }
    break;

  case 165:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_AUTO_INCREMENT); }
    break;

  case 166:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_PRIMARY_KEY); }
    break;

  case 167:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 168:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 169:

    {
      (yyval.node) = NULL;
    }
    break;

  case 170:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPIRE_INFO, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 171:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_MAX_SIZE, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 172:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_BLOCK_SIZE, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 173:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_REPLICA_NUM, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 174:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COMPRESS_METHOD, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 175:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_USE_BLOOM_FILTER, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 176:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSISTENT_MODE);
      (yyval.node)->value_ = 1;
    }
    break;

  case 177:

    {
      (void)((yyvsp[(1) - (4)].node)) ; /* make bison mute */
      (void)((yyvsp[(3) - (4)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHARSET, 1, (yyvsp[(4) - (4)].node));
    }
    break;

  case 178:

    {
      (void)((yyvsp[(1) - (5)].node)) ; /* make bison mute */
      (void)((yyvsp[(4) - (5)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHARSET, 1, (yyvsp[(5) - (5)].node));
    }
    break;

  case 179:

    { (yyval.node) = NULL; }
    break;

  case 180:

    { (yyval.node) = NULL; }
    break;

  case 181:

    { (yyval.node) = NULL; }
    break;

  case 182:

    { (yyval.node) = NULL; }
    break;

  case 183:

    {
      ParseNode *idx_columns = NULL;
      ParseNode *table_options = NULL;
      merge_nodes(idx_columns, result->malloc_pool_, T_INDEX_COLUMN_LIST, (yyvsp[(8) - (11)].node));
      merge_nodes(table_options, result->malloc_pool_, T_TABLE_OPTION_LIST, (yyvsp[(11) - (11)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_INDEX, 5,
              (yyvsp[(4) - (11)].node),                   /* index name */
              (yyvsp[(6) - (11)].node),                   /* table name */
              idx_columns,          /* index columns */
              (yyvsp[(10) - (11)].node),                  /* storing coumns */
              table_options         /* table option(s) */
              );
      (yyval.node)->value_ = (yyvsp[(2) - (11)].ival);              /* unique */
    }
    break;

  case 184:

    { (yyval.ival) = 1; }
    break;

  case 185:

    { (yyval.ival) = 0; }
    break;

  case 187:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 188:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 189:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SORT_COLUMN_KEY, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    }
    break;

  case 190:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_STORING_COLUMN_LIST, (yyvsp[(3) - (4)].node));
    }
    break;

  case 191:

    {
      (yyval.node) = NULL;
    }
    break;

  case 192:

    {
      ParseNode *tables = NULL;
      merge_nodes(tables, result->malloc_pool_, T_TABLE_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DROP_TABLE, 2, (yyvsp[(3) - (4)].node), tables);
    }
    break;

  case 193:

    { (yyval.node) = NULL; }
    break;

  case 194:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_IF_EXISTS); }
    break;

  case 195:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 196:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 197:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DROP_INDEX, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 198:

    {
    	ParseNode* val_list = NULL;
      merge_nodes(val_list, result->malloc_pool_, T_VALUE_LIST, (yyvsp[(6) - (6)].node));
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 5,
                              (yyvsp[(3) - (6)].node),           /* target relation */
                              (yyvsp[(4) - (6)].node),           /* column list */
                              val_list,     /* value list */
                              NULL,         /* value from sub-query */
                              (yyvsp[(1) - (6)].node)            /* is replacement */
                              );
    }
    break;

  case 199:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 5,
                              (yyvsp[(3) - (4)].node),           /* target relation */
                              NULL,         /* column list */
                              NULL,         /* value list */
                              (yyvsp[(4) - (4)].node),           /* value from sub-query */
                              (yyvsp[(1) - (4)].node)            /* is replacement */
                              );
    }
    break;

  case 200:

    {
      ParseNode* col_list = NULL;
      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(5) - (7)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 5,
                              (yyvsp[(3) - (7)].node),           /* target relation */
                              col_list,     /* column list */
                              NULL,         /* value list */
                              (yyvsp[(7) - (7)].node),           /* value from sub-query */
                              (yyvsp[(1) - (7)].node)            /* is replacement */
                              );
    }
    break;

  case 201:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    }
    break;

  case 202:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    }
    break;

  case 203:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(2) - (3)].node));
    }
    break;

  case 204:

    { (yyval.node) = NULL; }
    break;

  case 205:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 206:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 207:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(2) - (3)].node));
    }
    break;

  case 208:

    {
    merge_nodes((yyvsp[(4) - (5)].node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(4) - (5)].node));
    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (5)].node), (yyvsp[(4) - (5)].node));
  }
    break;

  case 209:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 210:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 211:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 212:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 213:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 214:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 215:

    {
      (yyval.node)= (yyvsp[(1) - (1)].node);
    }
    break;

  case 216:

    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
      (yyval.node)->children_[12] = (yyvsp[(2) - (2)].node);
    }
    break;

  case 217:

    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)(yyvsp[(1) - (3)].node);
      if (select->children_[10])
        destroy_tree(select->children_[10]);
      select->children_[10] = (yyvsp[(2) - (3)].node);
      select->children_[12] = (yyvsp[(3) - (3)].node);
      (yyval.node) = select;
    }
    break;

  case 218:

    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)(yyvsp[(1) - (4)].node);
      if ((yyvsp[(2) - (4)].node))
      {
        if (select->children_[10])
          destroy_tree(select->children_[10]);
        select->children_[10] = (yyvsp[(2) - (4)].node);
      }

      /* set limit value */
      if (select->children_[11])
        destroy_tree(select->children_[11]);
      select->children_[11] = (yyvsp[(3) - (4)].node);
      select->children_[12] = (yyvsp[(4) - (4)].node);
      (yyval.node) = select;
    }
    break;

  case 219:

    {
      ParseNode* project_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 14,
                              (yyvsp[(3) - (5)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              (yyvsp[(5) - (5)].node),             /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (5)].node)              /* 14 hints */
                              );
    }
    break;

  case 220:

    {
      ParseNode* project_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (8)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 14,
                              (yyvsp[(3) - (8)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              NULL,           /* 3. from clause */
                              (yyvsp[(7) - (8)].node),             /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              (yyvsp[(8) - (8)].node),             /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (8)].node)              /* 14 hints */
                              );
    }
    break;

  case 221:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 222:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 223:

    {
      ParseNode* project_list = NULL;
      ParseNode* from_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (9)].node));
      merge_nodes(from_list, result->malloc_pool_, T_FROM_LIST, (yyvsp[(6) - (9)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 14,
                              (yyvsp[(3) - (9)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              from_list,      /* 3. from clause */
                              (yyvsp[(7) - (9)].node),             /* 4. where */
                              (yyvsp[(8) - (9)].node),             /* 5. group by */
                              (yyvsp[(9) - (9)].node),             /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (9)].node)              /* 14 hints */
                              );
    }
    break;

  case 224:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_UNION);
	    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 14,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL            /* 14 hints */
                              );
    }
    break;

  case 225:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_INTERSECT);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 14,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL            /* 14 hints */
                              );
    }
    break;

  case 226:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_EXCEPT);
	    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 14,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL            /* 14 hints */
                              );
    }
    break;

  case 227:

    {(yyval.node) = NULL;}
    break;

  case 228:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 229:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHERE_CLAUSE, 2, (yyvsp[(3) - (3)].node), (yyvsp[(2) - (3)].node));
    }
    break;

  case 230:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 231:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(4) - (4)].node), (yyvsp[(2) - (4)].node));
    }
    break;

  case 232:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(2) - (2)].node), NULL);
    }
    break;

  case 233:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, NULL, (yyvsp[(2) - (2)].node));
    }
    break;

  case 234:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(4) - (4)].node), (yyvsp[(2) - (4)].node));
    }
    break;

  case 235:

    {
      (yyval.node) = NULL;
    }
    break;

  case 236:

    {
      if ((yyvsp[(2) - (3)].node))
      {
        merge_nodes((yyval.node), result->malloc_pool_, T_HINT_OPTION_LIST, (yyvsp[(2) - (3)].node));
      }
      else
      {
        (yyval.node) = NULL;
      }
    }
    break;

  case 237:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 238:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 239:

    {
      (yyval.node) = NULL;
    }
    break;

  case 240:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_READ_STATIC);
    }
    break;

  case 241:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_READ_CONSISTENCY);
      (yyval.node)->value_ = (yyvsp[(3) - (4)].ival);
    }
    break;

  case 242:

    { (yyval.node) = NULL; }
    break;

  case 243:

    { (yyval.ival) = 0; }
    break;

  case 244:

    { (yyval.ival) = 1; }
    break;

  case 245:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 246:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 247:

    { (yyval.node) = NULL; }
    break;

  case 248:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 249:

    { (yyval.node) = NULL; }
    break;

  case 250:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    }
    break;

  case 251:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 252:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 253:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 1;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 254:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 2;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 255:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    }
    break;

  case 256:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 1;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    }
    break;

  case 257:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 2;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    }
    break;

  case 258:

    { (yyval.node) = NULL; }
    break;

  case 259:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (3)].node));
    }
    break;

  case 260:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 261:

    { (yyval.node) = NULL; }
    break;

  case 262:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_SORT_LIST, (yyvsp[(3) - (3)].node));
    }
    break;

  case 263:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 264:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 265:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SORT_KEY, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    }
    break;

  case 266:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_ASC); }
    break;

  case 267:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_ASC); }
    break;

  case 268:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_DESC); }
    break;

  case 269:

    { (yyval.node) = 0; }
    break;

  case 270:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 271:

    {
      (yyval.node) = NULL;
    }
    break;

  case 272:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ALL);
    }
    break;

  case 273:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_DISTINCT);
    }
    break;

  case 274:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, (yyvsp[(1) - (1)].node));
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
    }
    break;

  case 275:

    {
      ParseNode* alias_node = NULL;
      malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (2)]).first_column, (yylsp[(1) - (2)]).last_column);
      dup_expr_string(alias_node->str_value_, result, (yylsp[(2) - (2)]).first_column, (yylsp[(2) - (2)]).last_column);
    }
    break;

  case 276:

    {
      ParseNode* alias_node = NULL;
      malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (3)]).first_column, (yylsp[(1) - (3)]).last_column);
      dup_expr_string(alias_node->str_value_, result, (yylsp[(3) - (3)]).first_column, (yylsp[(3) - (3)]).last_column);
    }
    break;

  case 277:

    {
      ParseNode* star_node = NULL;
      malloc_terminal_node(star_node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, star_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
    }
    break;

  case 278:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 279:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 280:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 281:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 282:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 283:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 284:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    }
    break;

  case 285:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 286:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    }
    break;

  case 287:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 288:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(2) - (5)].node), (yyvsp[(5) - (5)].node));
    	yyerror(&(yylsp[(1) - (5)]), result, "qualied joined table can not be aliased!");
      YYABORT;
    }
    break;

  case 289:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 290:

    {
    	(yyval.node) = (yyvsp[(2) - (3)].node);
    }
    break;

  case 291:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOINED_TABLE, 4, (yyvsp[(2) - (6)].node), (yyvsp[(1) - (6)].node), (yyvsp[(4) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 292:

    {
      ParseNode* node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_JOIN_INNER);
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOINED_TABLE, 4, node, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 293:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_FULL);
    }
    break;

  case 294:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_LEFT);
    }
    break;

  case 295:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_RIGHT);
    }
    break;

  case 296:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_INNER);
    }
    break;

  case 297:

    { (yyval.node) = NULL; }
    break;

  case 298:

    { (yyval.node) = NULL; }
    break;

  case 299:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPLAIN, 1, (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = ((yyvsp[(2) - (3)].node) ? 1 : 0); /* positive: verbose */
    }
    break;

  case 300:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 301:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 302:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 303:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 304:

    { (yyval.node) = (ParseNode*)1; }
    break;

  case 305:

    { (yyval.node) = NULL; }
    break;

  case 306:

    { 
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_TABLES, 1, (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(2) - (4)].ival);
    }
    break;

  case 307:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node)); }
    break;

  case 308:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node)); }
    break;

  case 309:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_TABLE_STATUS, 1, (yyvsp[(4) - (4)].node)); }
    break;

  case 310:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_SERVER_STATUS, 1, (yyvsp[(4) - (4)].node)); }
    break;

  case 311:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_VARIABLES, 1, (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(2) - (4)].ival);
    }
    break;

  case 312:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_SCHEMA); }
    break;

  case 313:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_CREATE_TABLE, 1, (yyvsp[(4) - (4)].node)); }
    break;

  case 314:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 315:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 316:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_WARNINGS, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 317:

    {
      if ((yyvsp[(2) - (3)].node)->type_ != T_FUN_COUNT)
      {
        yyerror(&(yylsp[(1) - (3)]), result, "Only COUNT(*) function is supported in SHOW WARNINGS statement!");
        YYABORT;
      }
      else
      {
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_WARNINGS);
        (yyval.node)->value_ = 1;
      }
    }
    break;

  case 318:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_GRANTS, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 319:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_PARAMETERS, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 320:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_INDEXES, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 321:

    {
      if ((yyvsp[(2) - (4)].node)->value_ < 0 || (yyvsp[(4) - (4)].node)->value_ < 0)
      {
        yyerror(&(yylsp[(1) - (4)]), result, "OFFSET/COUNT must not be less than 0!");
        YYABORT;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_LIMIT, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 322:

    {
      if ((yyvsp[(2) - (2)].node)->value_ < 0)
      {
        yyerror(&(yylsp[(1) - (2)]), result, "COUNT must not be less than 0!");
        YYABORT;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_LIMIT, 2, NULL, (yyvsp[(2) - (2)].node));
    }
    break;

  case 323:

    { (yyval.node) = NULL; }
    break;

  case 324:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 325:

    { (yyval.node) = NULL; }
    break;

  case 326:

    { (yyval.node) = NULL; }
    break;

  case 327:

    { (yyval.ival) = 1; }
    break;

  case 328:

    { (yyval.ival) = 0; }
    break;

  case 329:

    { (yyval.ival) = 0; }
    break;

  case 330:

    { (yyval.node) = NULL; }
    break;

  case 331:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 1, (yyvsp[(2) - (2)].node)); }
    break;

  case 332:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHERE_CLAUSE, 1, (yyvsp[(2) - (2)].node)); }
    break;

  case 333:

    { (yyval.node) = NULL; }
    break;

  case 334:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 1, (yyvsp[(1) - (1)].node)); }
    break;

  case 335:

    {
        merge_nodes((yyval.node), result->malloc_pool_, T_CREATE_USER, (yyvsp[(3) - (3)].node));
    }
    break;

  case 336:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 337:

    {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 338:

    {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_USER_SPEC, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 339:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 340:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 341:

    {
        merge_nodes((yyval.node), result->malloc_pool_, T_DROP_USER, (yyvsp[(3) - (3)].node));
    }
    break;

  case 342:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 343:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 344:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SET_PASSWORD, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 345:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SET_PASSWORD, 2, (yyvsp[(3) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 346:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 347:

    {
      (yyval.node) = NULL;
    }
    break;

  case 348:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_RENAME_USER, (yyvsp[(3) - (3)].node));
    }
    break;

  case 349:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_RENAME_INFO, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 350:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 351:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 352:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LOCK_USER, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 353:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    }
    break;

  case 354:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    }
    break;

  case 355:

    {
      (void)(yyval.node);
    }
    break;

  case 356:

    {
      (void)(yyval.node);
    }
    break;

  case 357:

    {
      (yyval.ival) = 1;
    }
    break;

  case 358:

    {
      (yyval.ival) = 0;
    }
    break;

  case 359:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BEGIN);
      (yyval.node)->value_ = 0;
    }
    break;

  case 360:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BEGIN);
      (yyval.node)->value_ = (yyvsp[(3) - (3)].ival);
    }
    break;

  case 361:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_COMMIT);
    }
    break;

  case 362:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ROLLBACK);
    }
    break;

  case 363:

    {
      ParseNode *privileges_node = NULL;
      ParseNode *users_node = NULL;
      merge_nodes(privileges_node, result->malloc_pool_, T_PRIVILEGES, (yyvsp[(2) - (6)].node));
      merge_nodes(users_node, result->malloc_pool_, T_USERS, (yyvsp[(6) - (6)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_GRANT,
                                 3, privileges_node, (yyvsp[(4) - (6)].node), users_node);
    }
    break;

  case 364:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 365:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 366:

    {
      (void)(yyvsp[(2) - (2)].node);                 /* useless */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_ALL;
    }
    break;

  case 367:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_ALTER;
    }
    break;

  case 368:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_CREATE;
    }
    break;

  case 369:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_CREATE_USER;
    }
    break;

  case 370:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_DELETE;
    }
    break;

  case 371:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_DROP;
    }
    break;

  case 372:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_GRANT_OPTION;
    }
    break;

  case 373:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_INSERT;
    }
    break;

  case 374:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_UPDATE;
    }
    break;

  case 375:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_SELECT;
    }
    break;

  case 376:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_REPLACE;
    }
    break;

  case 377:

    {
      (void)(yyval.node);
    }
    break;

  case 378:

    {
      (void)(yyval.node);
    }
    break;

  case 379:

    {
      /* means global priv_level */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_LEVEL);
    }
    break;

  case 380:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_LEVEL, 1, (yyvsp[(1) - (1)].node));
    }
    break;

  case 381:

    {
      ParseNode *privileges_node = NULL;
      ParseNode *priv_level = NULL;
      merge_nodes(privileges_node, result->malloc_pool_, T_PRIVILEGES, (yyvsp[(2) - (5)].node));
      if ((yyvsp[(3) - (5)].node) == NULL)
      {
        /* check privileges: should have and only have ALL PRIVILEGES, GRANT OPTION */
        int check_ok = 0;
        if (2 == privileges_node->num_child_)
        {
          assert(privileges_node->children_[0]->num_child_ == 0);
          assert(privileges_node->children_[0]->type_ == T_PRIV_TYPE);
          assert(privileges_node->children_[1]->num_child_ == 0);
          assert(privileges_node->children_[1]->type_ == T_PRIV_TYPE);
          if ((privileges_node->children_[0]->value_ == OB_PRIV_ALL
               && privileges_node->children_[1]->value_ == OB_PRIV_GRANT_OPTION)
              || (privileges_node->children_[1]->value_ == OB_PRIV_ALL
                  && privileges_node->children_[0]->value_ == OB_PRIV_GRANT_OPTION))
          {
            check_ok = 1;
          }
        }
        if (!check_ok)
        {
          yyerror(&(yylsp[(1) - (5)]), result, "support only ALL PRIVILEGES, GRANT OPTION");
          YYABORT;
        }
      }
      else
      {
        priv_level = (yyvsp[(3) - (5)].node);
      }
      ParseNode *users_node = NULL;
      merge_nodes(users_node, result->malloc_pool_, T_USERS, (yyvsp[(5) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_REVOKE,
                                 3, privileges_node, priv_level, users_node);
    }
    break;

  case 382:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 383:

    {
      (yyval.node) = NULL;
    }
    break;

  case 384:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PREPARE, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 385:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 386:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 387:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 388:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 389:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 390:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_VARIABLE_SET, (yyvsp[(2) - (2)].node));;
      (yyval.node)->value_ = 2;
    }
    break;

  case 391:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 392:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 393:

    {
      (void)((yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 394:

    {
      (void)((yyvsp[(2) - (3)].node));
      (yyvsp[(1) - (3)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 395:

    {
      (void)((yyvsp[(3) - (4)].node));
      (yyvsp[(2) - (4)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = 1;
    }
    break;

  case 396:

    {
      (void)((yyvsp[(3) - (4)].node));
      (yyvsp[(2) - (4)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 397:

    {
      (void)((yyvsp[(4) - (5)].node));
      (yyvsp[(3) - (5)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
      (yyval.node)->value_ = 1;
    }
    break;

  case 398:

    {
      (void)((yyvsp[(4) - (5)].node));
      (yyvsp[(3) - (5)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 399:

    {
      (void)((yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 400:

    { (yyval.node) = NULL; }
    break;

  case 401:

    { (yyval.node) = NULL; }
    break;

  case 402:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 403:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXECUTE, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 404:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(2) - (2)].node));
    }
    break;

  case 405:

    {
      (yyval.node) = NULL;
    }
    break;

  case 406:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 407:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 408:

    {
      (void)((yyvsp[(1) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DEALLOCATE, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 409:

    { (yyval.node) = NULL; }
    break;

  case 410:

    { (yyval.node) = NULL; }
    break;

  case 411:

    {
      ParseNode *alter_actions = NULL;
      merge_nodes(alter_actions, result->malloc_pool_, T_ALTER_ACTION_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_TABLE, 2, (yyvsp[(3) - (4)].node), alter_actions);
    }
    break;

  case 412:

    {
      yyerror(&(yylsp[(1) - (6)]), result, "Table rename is not supported now");
      YYABORT;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLE_RENAME, 1, (yyvsp[(6) - (6)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_ACTION_LIST, 1, (yyval.node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_TABLE, 2, (yyvsp[(3) - (6)].node), (yyval.node));
    }
    break;

  case 413:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 414:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 415:

    {
      (void)((yyvsp[(2) - (3)].node)); /* make bison mute */
      (yyval.node) = (yyvsp[(3) - (3)].node); /* T_COLUMN_DEFINITION */
    }
    break;

  case 416:

    {
      (void)((yyvsp[(2) - (4)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_DROP, 1, (yyvsp[(3) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(4) - (4)].ival);
    }
    break;

  case 417:

    {
      (void)((yyvsp[(2) - (4)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_ALTER, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 418:

    {
      (void)((yyvsp[(2) - (5)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_RENAME, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 419:

    { (yyval.node) = NULL; }
    break;

  case 420:

    { (yyval.node) = NULL; }
    break;

  case 421:

    { (yyval.ival) = 2; }
    break;

  case 422:

    { (yyval.ival) = 1; }
    break;

  case 423:

    { (yyval.ival) = 0; }
    break;

  case 424:

    {
      (void)((yyvsp[(3) - (3)].node)); /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NOT_NULL);
    }
    break;

  case 425:

    {
      (void)((yyvsp[(3) - (3)].node)); /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NULL);
    }
    break;

  case 426:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 427:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_NULL);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyval.node));
    }
    break;

  case 428:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_SYTEM_ACTION_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_SYSTEM, 1, (yyval.node));
    }
    break;

  case 429:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 430:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 431:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SYSTEM_ACTION, 5,
                               (yyvsp[(1) - (9)].node),    /* param_name */
                               (yyvsp[(3) - (9)].node),    /* param_value */
                               (yyvsp[(4) - (9)].node),    /* comment */
                               (yyvsp[(8) - (9)].node),    /* server type */
                               (yyvsp[(9) - (9)].node)     /* cluster or IP/port */
                               );
      (yyval.node)->value_ = (yyvsp[(5) - (9)].ival);                /* scope */
    }
    break;

  case 432:

    { (yyval.node) = (yyvsp[(2) - (2)].node); }
    break;

  case 433:

    { (yyval.node) = NULL; }
    break;

  case 434:

    {(yyval.ival)=1;}
    break;

  case 435:

    {(yyval.ival)=0;}
    break;

  case 436:

    { (yyval.ival) = 0; }
    break;

  case 437:

    { (yyval.ival) = 1; }
    break;

  case 438:

    { (yyval.ival) = 2; }
    break;

  case 439:

    { (yyval.ival) = 2; }
    break;

  case 440:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 1;
    }
    break;

  case 441:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 4;
    }
    break;

  case 442:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 2;
    }
    break;

  case 443:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 3;
    }
    break;

  case 444:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CLUSTER, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 445:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SERVER_ADDRESS, 2, (yyvsp[(3) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 446:

    { (yyval.node) = NULL; }
    break;

  case 447:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 448:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
      else
      {
        (yyval.node)->value_ = strlen((yyval.node)->str_value_);
      }
    }
    break;

  case 449:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 450:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
      else
      {
        (yyval.node)->value_ = strlen((yyval.node)->str_value_);
      }
    }
    break;

  case 452:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 453:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
    }
    break;



      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now `shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*------------------------------------.
| yyerrlab -- here on detecting error |
`------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (&yylloc, result, YY_("syntax error"));
#else
# define YYSYNTAX_ERROR yysyntax_error (&yymsg_alloc, &yymsg, \
                                        yyssp, yytoken)
      {
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = YYSYNTAX_ERROR;
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == 1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = (char *) YYSTACK_ALLOC (yymsg_alloc);
            if (!yymsg)
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = 2;
              }
            else
              {
                yysyntax_error_status = YYSYNTAX_ERROR;
                yymsgp = yymsg;
              }
          }
        yyerror (&yylloc, result, yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }

  yyerror_range[1] = yylloc;

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
	 error, discard it.  */

      if (yychar <= YYEOF)
	{
	  /* Return failure if at end of input.  */
	  if (yychar == YYEOF)
	    YYABORT;
	}
      else
	{
	  yydestruct ("Error: discarding",
		      yytoken, &yylval, &yylloc, result);
	  yychar = YYEMPTY;
	}
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  yyerror_range[1] = yylsp[1-yylen];
  /* Do not reclaim the symbols of the rule which action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;	/* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
	{
	  yyn += YYTERROR;
	  if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
	    {
	      yyn = yytable[yyn];
	      if (0 < yyn)
		break;
	    }
	}

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
	YYABORT;

      yyerror_range[1] = *yylsp;
      yydestruct ("Error: popping",
		  yystos[yystate], yyvsp, yylsp, result);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  *++yyvsp = yylval;

  yyerror_range[2] = yylloc;
  /* Using YYLLOC is tempting, but would change the location of
     the lookahead.  YYLOC is available though.  */
  YYLLOC_DEFAULT (yyloc, yyerror_range, 2);
  *++yylsp = yyloc;

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#if !defined(yyoverflow) || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, result, YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, &yylloc, result);
    }
  /* Do not reclaim the symbols of the rule which action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
		  yystos[*yyssp], yyvsp, yylsp, result);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  /* Make sure YYID is used.  */
  return YYID (yyresult);
}





void yyerror(YYLTYPE* yylloc, ParseResult* p, char* s, ...)
{
  if (p != NULL)
  {
    p->result_tree_ = 0;
    va_list ap;
    va_start(ap, s);
    vsnprintf(p->error_msg_, MAX_ERROR_MSG, s, ap);
    if (yylloc != NULL)
    {
      p->start_col_ = yylloc->first_column;
      p->end_col_ = yylloc->last_column;
      p->line_ = yylloc->first_line;
    }
  }
}

int parse_init(ParseResult* p)
{
  int ret = 0;  // can not include C++ file "ob_define.h"
  if (!p || !p->malloc_pool_)
  {
    ret = -1;
    if (p)
    {
      snprintf(p->error_msg_, MAX_ERROR_MSG, "malloc_pool_ must be set");
    }
  }
  if (ret == 0)
  {
    ret = yylex_init_extra(p, &(p->yyscan_info_));
  }
  return ret;
}

int parse_terminate(ParseResult* p)
{
  return yylex_destroy(p->yyscan_info_);
}

int parse_sql(ParseResult* p, const char* buf, size_t len)
{
  int ret = -1;
  p->result_tree_ = 0;
  p->error_msg_[0] = 0;
  p->input_sql_ = buf;
  p->start_col_ = 1;
  p->end_col_ = 1;
  p->line_ = 1;
  p->yycolumn_ = 1;
  p->yylineno_ = 1;

  if (buf == NULL || len <= 0)
  {
    snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be empty");
    return ret;
  }

  while(len > 0 && isspace(buf[len - 1]))
    --len;

  if (len <= 0)
  {
    snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be while space only");
    return ret;
  }

  YY_BUFFER_STATE bp;

  //bp = yy_scan_string(buf, p->yyscan_info_);
  bp = yy_scan_bytes(buf, len, p->yyscan_info_);
  yy_switch_to_buffer(bp, p->yyscan_info_);
  ret = yyparse(p);
  yy_delete_buffer(bp, p->yyscan_info_);
  return ret;
}

