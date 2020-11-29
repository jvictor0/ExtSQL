create database if not exists ext_sql;
use ext_sql;

create table if not exists serre_cartan_elts
(
    id bigint unsigned auto_increment not null,
    squares blob not null,    
    grade bigint unsigned not null,
    trailing_squares_id bigint not null,
    leading_square as (ascii(squares) << 8) | ascii(substring(squares, 2, 1)) persisted bigint unsigned,
    trailing_squares as substring(squares, 3) persisted blob,
    key(grade, id) using clustered columnstore,
    unique key(id) unenforced norely,
    shard(id)
);

delimiter //

create or replace function number_to_two_byte_blob(num bigint unsigned)
returns blob not null
as
begin
    return concat(char(num >> 8), char(num & 255));
end//

delimiter ;

create table if not exists nonzero_binomial_coefs
(
    i int not null,
    j int not null,
    k int not null,
    i_plus_j_minus_k_square bigint not null,
    i_square bigint not null,
    k_square bigint not null,
    primary key(i,j,k)
);

-- we store this table de-normalized because typing the joins every time is error prone
-- 
create table if not exists steenrod_products
(
    lhs_id bigint unsigned not null,
    lhs_squares blob not null,
    lhs_grade bigint unsigned not null,    
    lhs_leading_square as (ascii(lhs_squares) << 8) | ascii(substring(lhs_squares, 2, 1)) persisted bigint unsigned,
    lhs_trailing_squares as substring(lhs_squares, 3) persisted blob,

    rhs_id bigint unsigned not null,
    rhs_squares blob not null,
    rhs_grade bigint unsigned not null,    
    rhs_leading_square as (ascii(rhs_squares) << 8) | ascii(substring(rhs_squares, 2, 1)) persisted bigint unsigned,
    rhs_trailing_squares as substring(rhs_squares, 3) persisted blob,
    
    prod_id bigint unsigned not null,
    prod_squares blob not null,
    prod_grade as lhs_grade + rhs_grade persisted bigint unsigned,    
    prod_leading_square as (ascii(prod_squares) << 8) | ascii(substring(prod_squares, 2, 1)) persisted bigint unsigned,
    prod_trailing_squares as substring(prod_squares, 3) persisted blob,

    is_trivial as prod_squares = concat(lhs_squares, rhs_squares) persisted tinyint,

    unique key(lhs_id, rhs_id, prod_id) unenforced norely,

    key (rhs_grade, lhs_grade, rhs_id, lhs_id, prod_id) using clustered columnstore,
    shard(rhs_id)
);

create table if not exists steenrods_computed
(
    grade bigint primary key
);

create table if not exists resolution_ids
(
    id bigint unsigned auto_increment not null,
    grade bigint unsigned not null,
    dimension bigint unsigned not null,
    from_col_ix bigint unsigned,
    shard(id),
    key(from_col_ix, id) using clustered columnstore
);

create table if not exists resolution_generators
(
    id bigint unsigned not null,
    grade bigint unsigned not null,
    dimension bigint unsigned not null,
    differential_gen bigint unsigned not null,
    differential_square bigint unsigned not null,
    shard(id),
    key(dimension, grade, id) using clustered columnstore
);

delimiter //

create or replace function product_to_res_id(gen_id bigint unsigned, square_id bigint unsigned)
returns bigint unsigned
as
begin
    return (gen_id << 48) | square_id;
end//

create or replace function res_id_to_gen_id(res_id bigint unsigned)
returns bigint unsigned
as
begin
    return res_id >> 48;
end//

create or replace function res_id_to_sq_id(res_id bigint unsigned)
returns bigint unsigned
as
begin
    return (res_id << 16) >> 16;
end//

create or replace function res_id_set_kernel(res_id bigint unsigned)
returns bigint unsigned
as
begin
    return res_id | (1 << 63);
end //

create or replace function res_id_unset_kernel(res_id bigint unsigned)
returns bigint unsigned
as
begin
    return res_id & (~(1 << 63));
end //

create or replace function res_id_is_kernel(res_id bigint unsigned)
returns bigint unsigned
as
begin
    return (res_id & (1 << 63)) != 0;
end //

delimiter ;

create table if not exists resolution_matrix
(
    col_ix bigint unsigned not null,     
    row_ix bigint unsigned not null,
    leading_ix bigint unsigned not null,
    iteration bigint unsigned not null default 0,
    shard(leading_ix),
    key(leading_ix, col_ix, row_ix) using clustered columnstore
);

create table if not exists cycles_matrix
(
    col_ix bigint unsigned not null,     
    row_ix bigint unsigned not null,
    leading_ix bigint unsigned not null,
    iteration bigint unsigned not null default 0,
    shard(leading_ix),
    key(leading_ix, col_ix, row_ix) using clustered columnstore
);
    
