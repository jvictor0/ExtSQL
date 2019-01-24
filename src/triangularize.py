from memsql import joyo_utils
from util import *

def CreateTableQuery(namespace):
    return Dedent("""
        create table if not exists %(ns)s_matrix
        (
            col_ix bigint not null,
            row_ix bigint not null,
            leading_ix bigint not null,
            iteration bigint not null default 0,
            shard(leading_ix),
            key(leading_ix, col_ix, row_ix) using clustered columnstore
        )""" % {"ns": namespace})


def PrePivotsQuery(namespace, itr):
    return Dedent("""
                     select
                         col_ix,
                         row_ix,
                         leading_ix,
                         min(col_ix) over (partition by leading_ix) as min_col_ix
                     from %(ns)s_matrix
                     where iteration = %(itr)s""" % {"ns":namespace, "itr":itr})

def PivotsQuery(namespace, itr):
    return Dedent("""
                     select
                        pre_pivots.col_ix,
                        cols.col_ix as target_col_ix,
                        pre_pivots.row_ix,
                        pre_pivots.leading_ix
                     from 
                     (%(pre_pivots)s) pre_pivots
                     join (select distinct leading_ix, col_ix from %(ns)s_matrix where iteration = %(itr)s) cols
                     on pre_pivots.leading_ix = cols.leading_ix 
                     where min_col_ix = pre_pivots.col_ix""" % {
                         "pre_pivots":Indent(PrePivotsQuery(namespace, itr)),
                         "ns":namespace,
                         "itr":itr})


def NewVectorsQuery(namespace, itr):
    return Dedent("""
                    select
                         coalesce(matrix.col_ix, pivots.target_col_ix) as col_ix,
                         coalesce(matrix.row_ix, pivots.row_ix) as row_ix,
                         min(coalesce(matrix.row_ix, pivots.row_ix)) over (partition by matrix.col_ix) as leading_ix,
                         %(itr)s + 1 as iteration
                    from
                    (%(pivots)s) pivots
                    full outer join %(ns)s_matrix matrix
                    on matrix.row_ix = pivots.row_ix
                       and matrix.leading_ix = pivots.leading_ix
                       and matrix.col_ix = pivots.target_col_ix
                    where (iteration = %(itr)s or iteration is null)
                      and (matrix.col_ix is null or pivots.target_col_ix is null or matrix.col_ix = pivots.col_ix)""" % {
                          "ns": namespace,
                          "itr" : itr,
                          "pivots" : Indent(PivotsQuery(namespace, itr))
                      })
                  
def DoIteration(con, namespace, itr):
    if con is None:
        con = joyo_utils.ConnectToMemSQL("127.0.0.1:10000", database="ext_sql")
    con.query("insert into %(ns)s_matrix\n%(new_vectors)s" % {"ns":namespace, "new_vectors":NewVectorsQuery(namespace, itr)})


def Triangularize(con, namespace):
    if con is None:
        con = joyo_utils.ConnectToMemSQL("127.0.0.1:10000", database="ext_sql")
    con.query("delete from %(ns)s_matrix where iteration > 0" % {"ns":namespace})
    iteration = 0
    need_to_continue = True
    while need_to_continue:
        DoIteration(con, namespace, iteration)
        iteration += 1
        rows = con.query("select leading_ix, count(distinct leading_ix, col_ix) c from %(ns)s_matrix where iteration = %(itr)s group by leading_ix" % {"ns":namespace, "itr":iteration})
        need_to_continue = False
        for r in rows:
            if int(r["c"]) > 1:
                need_to_continue = True

def ListsToDb(con, namespace, lists):
    if con is None:
        con = joyo_utils.ConnectToMemSQL("127.0.0.1:10000", database="ext_sql")
    con.query(CreateTableQuery(namespace))
    con.query("delete from %(ns)s_matrix" % {"ns":namespace})
    lts = {}
    for row_ix, col in enumerate(lists):        
        for col_ix, r in enumerate(col):
            if r == 1:
                if col_ix not in lts:
                    lts[col_ix] = row_ix
                con.query("insert into %s_matrix (col_ix, row_ix, leading_ix) values (%d, %d, %d)" % (namespace, col_ix, row_ix, lts[col_ix]))

def DbToLists(con, namespace, itr=0):
    if con is None:
        con = joyo_utils.ConnectToMemSQL("127.0.0.1:10000", database="ext_sql")
    pre_result = {}
    for r in con.query("select * from %(ns)s_matrix where iteration = %(itr)s" % {'ns':namespace, "itr":itr}):
        pre_result[(int(r["col_ix"]), int(r["row_ix"]))] = 1
    num_rows = max([a for a,b in pre_result.keys()]) + 1
    num_cols = max([b for a,b in pre_result.keys()]) + 1
    result = []
    for col_ix in xrange(num_cols):
        result.append([])
        for row_ix in xrange(num_rows):
            if (row_ix, col_ix) in pre_result:
                result[-1].append(1)
            else:
                result[-1].append(0)
    return result

def PrintLists(result):
    print "\n".join([str(r) for r in result])
    
