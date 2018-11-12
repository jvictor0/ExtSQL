create database if not exists ext_sql;
use ext_sql;

create table if not exists serre_cartan_elts
(
    id bigint auto_increment not null,
    squares blob not null,    
    grade bigint not null,
    leading_square as (ascii(squares) << 8) | ascii(substring(squares, 2, 1)) persisted bigint,
    trailing_squares as substring(squares, 3) persisted blob,
    key(squares, id) using clustered columnstore,
    shard(squares)
);

delimiter //

create or replace function number_to_two_byte_blob(num bigint)
returns blob not null
as
begin
    return concat(char(num >> 8), char(num & 255));
end//

delimiter ;

create table if not exists nonzero_binomial_coefs(
    i int not null,
    j int not null,
    k int not null,
    primary key(i,j,k)
);

-- we store this table de-normalized because typing the joins every time is error prone
-- 
create table if not exists steenrod_products(
    lhs_id bigint not null,
    lhs_squares blob not null,
    lhs_grade bigint not null,    
    lhs_leading_square as (ascii(lhs_squares) << 8) | ascii(substring(lhs_squares, 2, 1)) persisted bigint,
    lhs_trailing_squares as substring(lhs_squares, 3) persisted blob,

    rhs_id bigint not null,
    rhs_squares blob not null,
    rhs_grade bigint not null,    
    rhs_leading_square as (ascii(rhs_squares) << 8) | ascii(substring(rhs_squares, 2, 1)) persisted bigint,
    rhs_trailing_squares as substring(rhs_squares, 3) persisted blob,
    
    prod_id bigint not null,
    prod_squares blob not null,
    prod_grade as lhs_grade + rhs_grade persisted bigint,    
    prod_leading_square as (ascii(prod_squares) << 8) | ascii(substring(prod_squares, 2, 1)) persisted bigint,
    prod_trailing_squares as substring(prod_squares, 3) persisted blob,  

    key (rhs_id, lhs_id, prod_id) using clustered columnstore,
    shard(rhs_id)
);

