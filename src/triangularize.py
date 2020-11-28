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


def ActiveColsQuery(namespace, itr, min_leading_ix):
    return Dedent("""
                     select
                         col_ix,
                         row_ix,
                         leading_ix,
                         pivot_col_ix
                     from
                     (
                         select
                             col_ix,
                             row_ix,
                             leading_ix,
                             min(col_ix) over (partition by leading_ix) as pivot_col_ix,
                             count(*) over (partition by leading_ix) as num_elts_in_group,
                             count(*) over (partition by leading_ix, col_ix) as num_elts_in_col
                         from %(ns)s_matrix
                         where leading_ix >= %(min_leading_ix)s
                     ) sub
                     where num_elts_in_group != num_elts_in_col""" % {
                         "ns":namespace,
                         "itr":itr,
                         "min_leading_ix": min_leading_ix
                     })

def PivotsQueryUnderCTE(namespace, itr, min_leading_ix):
    return Dedent("""
                     select
                        cols.col_ix as col_ix,
                        active_cols.row_ix,
                        active_cols.leading_ix
                     from 
                     active_cols
                     join (select distinct leading_ix, col_ix from %(ns)s_matrix where leading_ix >= %(min_leading_ix)s) cols
                     on active_cols.leading_ix = cols.leading_ix 
                        and active_cols.col_ix != cols.col_ix
                     where pivot_col_ix = active_cols.col_ix""" % {
                         "ns" : namespace,
                         "itr" : itr,
                         "min_leading_ix" : min_leading_ix
                     })

def DeleteNonPivotsQuery(namespace, itr, min_leading_ix):
    return Dedent("""
                   delete %(ns)s_matrix 
                   from %(ns)s_matrix 
                   left join
                   (
                         select 
                             leading_ix,
                             min(col_ix) piv_col
                         from %(ns)s_matrix 
                         where leading_ix >= %(min_leading_ix)s
                           and iteration <= %(itr)s
                         group by leading_ix
                   ) piv_cols
                   on piv_cols.leading_ix = %(ns)s_matrix.leading_ix
                      and %(ns)s_matrix.col_ix = piv_cols.piv_col
                   where piv_cols.piv_col is null
                      and %(ns)s_matrix.iteration <= %(itr)s""" % {
                          "ns" : namespace,
                          "itr" : itr,
                          "min_leading_ix" : min_leading_ix
                      })


def NewVectorsQueryUnderCTE(namespace, itr, min_leading_ix):
    return Dedent("""
                  select
                      col_ix,
                      row_ix,
                      min(row_ix) over (partition by leading_ix, col_ix) as leading_ix,
                      %(itr)s + 1 as iteration
                  from
                  (
                     %(pivots)s
                     union all
                     select 
                        col_ix,
                        row_ix,
                        leading_ix 
                     from active_cols 
                     where pivot_col_ix != col_ix
                  ) sub
                  group by row_ix, col_ix, leading_ix
                  having count(*) %% 2 = 1
                  """ % {
                      "pivots": Indent(PivotsQueryUnderCTE(namespace, itr, min_leading_ix)),
                      "itr": itr
                  })

def InsertNewVectorsQuery(namespace, itr, min_leading_ix):
    return Dedent("""
                    with active_cols as (%(active_cols)s)
                    insert into %(ns)s_matrix
                    %(new_vectors)s""" % {
                        "ns":namespace,
                        "new_vectors":NewVectorsQueryUnderCTE(namespace, itr, min_leading_ix),
                        "active_cols": Indent(ActiveColsQuery(namespace, itr, min_leading_ix))})

def DoIteration(con, namespace, itr, min_leading_ix):
    if con is None:
        con = ConnectToMemSQL()
    con.query(InsertNewVectorsQuery(namespace, itr, min_leading_ix))
    con.query(DeleteNonPivotsQuery(namespace, itr, min_leading_ix))


def DoneQuery(namespace, iteration, min_leading_ix):
    return """select min(leading_ix) c from (
                  select leading_ix from %(ns)s_matrix
                  where leading_ix >= %(min_leading_ix)s
                  group by leading_ix 
                  having count(distinct leading_ix, col_ix) > 1) sub""" % {
                      "ns":namespace,
                      "itr":iteration,
                      "min_leading_ix" : min_leading_ix
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
    min_leading_ix = 0
    while need_to_continue:
        DoIteration(con, namespace, iteration, min_leading_ix)
        SanityCheck(con, namespace)
        iteration += 1
        rows = con.query(DoneQuery(namespace, iteration, min_leading_ix))
        need_to_continue = rows[0]['c'] is not None
        if need_to_continue:
            min_leading_ix = int(rows[0]['c'])

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
                            ["cur_itr bigint not null",
                             "min_leading_ix bigint not null"],
                            "bigint",
                            mr_sp.Declare(["q query(c tinyint) = %s;" % DoneQuery(namespace, "cur_itr", "min_leading_ix")]),
                            mr_sp.Body(["return scalar(q);"]))
                            
        
def GenMainStoredProc(namespace):
    result = mr_sp.StoredProc("triangularize_" + namespace,
                              [],
                              None,
                              mr_sp.Declare(["cur_itr bigint not null = 0;",
                                             "is_done bigint = 0;",
                                             "min_leading_ix bigint not null = 0;"]),
                              mr_sp.Body(["delete from %s_matrix where iteration > 0;" % namespace,
                                          mr_sp.While("is_done is not null",
                                                      [
                                                          "min_leading_ix = is_done;",
                                                          InsertNewVectorsQuery(namespace, "cur_itr", "min_leading_ix") + ";",
                                                          DeleteNonPivotsQuery(namespace, "cur_itr", "min_leading_ix") + ";",
                                                          "cur_itr = cur_itr + 1;",
                                                          "is_done = " + namespace + "_is_done(cur_itr, min_leading_ix);"
                                                      ])]))
    return result
                                        
def GenStoredProcs(namespace):
    return [GenDoneProc(namespace), GenMainStoredProc(namespace)]

def CreateStoredProcs(con, namespace):
    for p in GenStoredProcs(namespace):
        print p.ToSQL()
        con.query(p.ToSQL())

