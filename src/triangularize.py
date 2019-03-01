from util import *
import mr_sp

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
                     from %(ns)s_matrix""" % {"ns":namespace, "itr":itr})

def PivotsQuery(namespace, itr):
    return Dedent("""
                     select
                        pre_pivots.col_ix,
                        cols.col_ix as target_col_ix,
                        pre_pivots.row_ix,
                        pre_pivots.leading_ix
                     from 
                     (%(pre_pivots)s) pre_pivots
                     join (select distinct leading_ix, col_ix from %(ns)s_matrix) cols
                     on pre_pivots.leading_ix = cols.leading_ix 
                     where min_col_ix = pre_pivots.col_ix""" % {
                         "pre_pivots":Indent(PrePivotsQuery(namespace, itr)),
                         "ns":namespace,
                         "itr":itr})

def DeleteNonPivotsQuery(namespace, itr):
    return Dedent("""
                   delete %(ns)s_matrix 
                   from %(ns)s_matrix 
                   left join
                   (
                         select 
                             leading_ix,
                             min(col_ix) piv_col
                         from %(ns)s_matrix 
                         where iteration <= %(itr)s
                         group by leading_ix
                   ) piv_cols
                   on piv_cols.leading_ix = %(ns)s_matrix.leading_ix
                      and %(ns)s_matrix.col_ix = piv_cols.piv_col
                   where piv_cols.piv_col is null
                      and %(ns)s_matrix.iteration <= %(itr)s""" % {
                       "ns":namespace,
                       "itr":itr})


def NewVectorsQuery(namespace, itr):
    return Dedent("""
                    select
                         coalesce(matrix.col_ix, pivots.target_col_ix) as col_ix,
                         coalesce(matrix.row_ix, pivots.row_ix) as row_ix,
                         min(coalesce(matrix.row_ix, pivots.row_ix)) over (partition by coalesce(matrix.col_ix, pivots.target_col_ix)) as leading_ix,
                         %(itr)s + 1 as iteration
                    from
                    (%(pivots)s) pivots
                    full outer join %(ns)s_matrix matrix
                    on matrix.row_ix = pivots.row_ix
                       and matrix.leading_ix = pivots.leading_ix
                       and matrix.col_ix = pivots.target_col_ix
                    where matrix.col_ix is null 
                       or pivots.target_col_ix is null""" % {
                          "ns": namespace,
                          "itr" : itr,
                          "pivots" : Indent(PivotsQuery(namespace, itr))
                      })
                  
def DoIteration(con, namespace, itr):
    if con is None:
        con = ConnectToMemSQL()
    con.query("insert into %(ns)s_matrix\n%(new_vectors)s" % {"ns":namespace, "new_vectors":NewVectorsQuery(namespace, itr)})
    con.query(DeleteNonPivotsQuery(namespace, itr))


def DoneQuery(namespace, iteration):
    return """select count(*) c from (
                  select leading_ix from %(ns)s_matrix 
                  where iteration = %(itr)s 
                  group by leading_ix 
                  having count(distinct leading_ix, col_ix) > 1) sub""" % {
                      "ns":namespace,
                      "itr":iteration
                  }
    
def Triangularize(con, namespace, use_sp=False):
    if con is None:
        con = ConnectToMemSQL()
    if use_sp:
        con.query("call triangularize_%s()" % namespace)
        return
    con.query("delete from %(ns)s_matrix where iteration > 0" % {"ns":namespace})
    iteration = 0
    need_to_continue = True
    while need_to_continue:
        DoIteration(con, namespace, iteration)
        SanityCheck(con, namespace)
        iteration += 1
        rows = con.query(DoneQuery(namespace, iteration))
        need_to_continue = int(rows[0]['c']) > 0

def SanityCheck(con, namespace):
    assert len(con.query("select * from %(ns)s_matrix group by iteration, row_ix, col_ix having count(*) > 1" % {"ns" : namespace})) == 0
    assert len(con.query("select col_ix, iteration, count(distinct leading_ix) from %(ns)s_matrix group by 1,2 having count(distinct leading_ix) > 1" % {"ns" : namespace})) == 0
    assert len(con.query("select * from %(ns)s_matrix group by col_ix having count(distinct iteration) > 1" % {"ns": namespace})) == 0
        
def ListsToDb(con, namespace, lists):
    if con is None:
        con = ConnectToMemSQL()
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
        con = ConnectToMemSQL()
    pre_result = {}
    for r in con.query("select * from %(ns)s_matrix where iteration = %(itr)s" % {'ns':namespace, "itr":itr}):
        pre_result[(int(r["col_ix"]), int(r["row_ix"]))] = 1
    row_list = sorted(list(set([a for a,b in pre_result.keys()])))
    col_list = sorted(list(set([b for a,b in pre_result.keys()])))
    result = []
    for col_ix in col_list:
        result.append([])
        for row_ix in row_list:
            if (row_ix, col_ix) in pre_result:
                result[-1].append(1)
            else:
                result[-1].append(0)
    return result

def PrintLists(result):
    print "\n".join([" ".join(r) for r in result])
    
def GenDoneProc(namespace):
    return mr_sp.StoredProc(namespace + "_is_done",
                            ["cur_itr bigint not null"],
                            "tinyint not null",
                            mr_sp.Declare(["q query(c tinyint) = %s;" % DoneQuery(namespace, "cur_itr")]),
                            mr_sp.Body(["return scalar(q) = 0;"]))
                            
        
def GenMainStoredProc(namespace):
    result = mr_sp.StoredProc("triangularize_" + namespace,
                              [],
                              None,
                              mr_sp.Declare(["cur_itr bigint not null = 0;",
                                             "is_done tinyint not null = 0;"]),
                              mr_sp.Body(["delete from %s_matrix where iteration > 0;" % namespace,
                                          mr_sp.While("not is_done",
                                                      ["insert into %(ns)s_matrix\n%(new_vectors)s;"
                                                       % {"ns":namespace, "new_vectors":NewVectorsQuery(namespace, "cur_itr")},
                                                       DeleteNonPivotsQuery(namespace, "cur_itr") + ";",
                                                       "cur_itr = cur_itr + 1;",
                                                       "is_done = " + namespace + "_is_done(cur_itr);"])]))
    return result
                                        
def GenStoredProcs(namespace):
    return [GenDoneProc(namespace), GenMainStoredProc(namespace)]

def CreateStoredProcs(con, namespace):
    for p in GenStoredProcs(namespace):
        print p.ToSQL()
        con.query(p.ToSQL())

