/* A Bison parser, made by GNU Bison 2.5.  */

/* Bison interface for Yacc-like parsers in C
   
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



