create table i01xi01(pk1 int primary key, a1 int);
create table i04xi04(pk1 int, pk2 int, pk3 int, pk4 int, a1 int, a2 int, a3 int, a4 int, primary key (pk1,pk2,pk3,pk4));
create table s01xs01(pk1 varchar(32) primary key, a1 varchar(32));
create table mix(pk1 int, pk2 int, pk3 int, pk4 int, pk5 varchar(32), a1 int, a2 int, a3 int, a4 int, a5 varchar(32),
primary key (pk1,pk2,pk3,pk4,pk5));
create table mcols16(pk1 int, pk2 int, pk3 int, pk4 int,
a1 int, a2 int, a3 int, a4 int, a5 int, a6 int, a7 int, a8 int,
a9 int, a10 int, a11 int, a12 int, a13 int, a14 int, a15 int, a16 int,
primary key (pk1,pk2,pk3,pk4)
);

create table kctable1(opt_type INT, opt_num INT, inventory_id INT, user_id INT, item_id INT, sku_id INT, bizorderid INT, gmt_create INT, gmt_modified INT, childbizorderid INT, quantity INT, status INT, version INT, time_out INT, time_number INT, opt_gmt_create INT, area_id INT, feature VARCHAR, flag INT, store_code VARCHAR, reserved_by_cache INT, store_quantity INT, reserved_quantity INT, PRIMARY KEY(item_id, sku_id, opt_num, opt_type));
create table kctable2(user_id INT , item_id INT, sku_id INT, type INT, store_code VARCHAR, quantity INT, reserve_quantity INT, gmt_create INT, gmt_modified INT, cofirm_quantity INT, version INT, is_deleted INT, status INT, feature VARCHAR, user_nick VARCHAR, PRIMARY KEY(item_id, sku_id, type, store_code));

-- create table kctable1(opt_type INT, opt_num INT, inventory_id INT, user_id INT, item_id INT, sku_id INT, bizorderid INT, gmt_create DATETIME, gmt_modified DATETIME, childbizorderid INT, quantity INT, status INT, version INT, time_out DATETIME, time_number INT, opt_gmt_create DATETIME, area_id INT, feature VARCHAR, flag INT, store_code VARCHAR, reserved_by_cache INT, store_quantity INT, reserved_quantity INT, PRIMARY KEY(item_id, sku_id, opt_num, opt_type));
-- create table kctable2(user_id INT , item_id INT, sku_id INT, type INT, store_code VARCHAR, quantity INT, reserve_quantity INT, gmt_create DATETIME, gmt_modified DATETIME, cofirm_quantity INT, version INT, is_deleted INT, status INT, feature VARCHAR, user_nick VARCHAR, PRIMARY KEY(item_id, sku_id, type, store_code));

-- insert into kctable2 values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) when row_count(update kctable2 set version=version+1, gmt_modified=0, quantity=0, reserve_quantity=reserve_quantity + 1 where item_id=$rk and sku_id=0 and type=0 and store_code=0 and (1000000000 - reserve_quantity - 1) >= 0)=1
