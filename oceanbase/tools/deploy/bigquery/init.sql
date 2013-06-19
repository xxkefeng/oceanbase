drop table if exists bigquery_prefix_rule;
create table bigquery_prefix_rule (
    prefix int,
    rule_data varchar(256),
    stat int,
    row_num int,
    primary key(prefix));

drop table if exists bigquery_table;
create table bigquery_table (
    prefix int,
    suffix int,
    col1 int,
    col2 int,
    col3 int,
    col4 int,
    misc varchar(1024),
    primary key(prefix, suffix)) tablet_max_size=4194304;

