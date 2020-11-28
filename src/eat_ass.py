from util import *
import mr_sp
import triangularize
import steenrod_gen
import os

def ResolutionDifferentialQuery(grade, dimension):
    return Dedent("""
    select
        product_to_res_id(gen.id, sq.id) as col_ix,
        product_to_res_id(gen.differential_gen, products.prod_id) as row_ix,
        min(product_to_res_id(gen.differential_gen, products.prod_id)) over (partition by product_to_res_id(gen.id, sq.id)) as leading_ix,
        0 as iteration
    from resolution_generators gen
    join serre_cartan_elts sq
    on sq.grade = %(grade)s - gen.grade
    join steenrod_products products 
    on products.rhs_id = gen.differential_square
       and products.lhs_id = sq.id
    where gen.dimension = %(dimension)s
    group by gen.id, sq.id, gen.differential_gen, products.prod_id
    having count(*) %% 2 = 1""" % {
        "dimension" : dimension,
        "grade" : grade
    })

def ResolutionBasisQuery(grade, dimension):
    return Dedent("""
    select
        product_to_res_id(gen.id, sq.id) as col_ix,
        res_id_set_kernel(product_to_res_id(gen.id, sq.id)) as row_ix,
        coalesce(any_value(matrix.leading_ix), res_id_set_kernel(product_to_res_id(gen.id, sq.id))) as leading_ix,
        0 as iteration
    from resolution_generators gen
    join serre_cartan_elts sq
    on sq.grade = %(grade)s - gen.grade
    left join resolution_matrix matrix
    on product_to_res_id(gen.id, sq.id) = matrix.col_ix
    where gen.dimension = %(dimension)s
    group by gen.id, sq.id""" % {
        "dimension" : dimension,
        "grade" : grade
    })

def CollectKernelQuery():
    return Dedent("""
    select
        res_id_set_kernel(col_ix),
        res_id_unset_kernel(row_ix),
        res_id_unset_kernel(leading_ix),
        0 as iteration
    from resolution_matrix matrix
    where res_id_is_kernel(leading_ix)""")

def CheckHomogeneousKernelQuery(dimension):
    return Dedent("""
    select
        *,
        res_id_to_gen_id(leading_ix)
    from cycles_matrix matrix
    join resolution_ids gens
    on res_id_to_gen_id(matrix.row_ix) = gens.id
    where gens.dimension != %(dimension)s""" % {
        "dimension" : dimension
    })

def CheckCyclesQuery():
    return Dedent("""
    select
        *
    from cycles_matrix cycs
    join resolution_generators gens
    on res_id_to_gen_id(cycs.row_ix) = gens.id
    join steenrod_products products
    on products.rhs_id = gens.differential_square
       and products.lhs_id = res_id_to_sq_id(cycs.row_ix)
    group by cycs.col_ix, gens.differential_gen, products.prod_id
    having count(*) % 2 = 1""")

def CollectImageQuery():
    return Dedent("""
    select
        col_ix,
        row_ix,
        leading_ix,
        0 as iteration
    from resolution_matrix matrix
    where not res_id_is_kernel(row_ix)""")    

def CollectNewGeneratorsQuery(grade, dimension):
    return Dedent("""
    select
        resolution_ids.id as id,
        %(grade)s as grade,
        %(dimension)s as dimension,
        res_id_to_gen_id(matrix.row_ix) as differential_gen,
        res_id_to_sq_id(matrix.row_ix) as differential_square
    from cycles_matrix matrix
    join resolution_ids
    on resolution_ids.from_col_ix = matrix.col_ix""" % {
        "dimension" : dimension,
        "grade" : grade
    })                

def CollectNewGeneratorIdsQuery(grade, dimension):
    return Dedent("""
    select
        %(grade)s as grade,
        %(dimension)s as dimension,
        col_ix as from_col_ix
    from cycles_matrix
    where res_id_is_kernel(col_ix)
    group by col_ix""" % {
        "dimension" : dimension,
        "grade" : grade
    })                

def PopulateResolutionMatrix(con, grade, dimension):
    con.query("delete from resolution_matrix")
    con.query("insert into resolution_matrix(col_ix, row_ix, leading_ix, iteration) %s" % ResolutionDifferentialQuery(grade, dimension))
    triangularize.SanityCheck(con, "resolution")
    con.query("insert into resolution_matrix(col_ix, row_ix, leading_ix, iteration) %s" % ResolutionBasisQuery(grade, dimension))
    triangularize.SanityCheck(con, "resolution")


def PopulateCyclesMatrixImage(con, dimension):
    con.query("insert into cycles_matrix(col_ix, row_ix, leading_ix, iteration) %s" % CollectImageQuery())
    assert len(con.query("select *, res_id_is_kernel(row_ix) from cycles_matrix where res_id_is_kernel(row_ix)")) == 0
    assert len(con.query(CheckCyclesQuery())) == 0, con.query(CheckCyclesQuery()).format_table()

def PopulateCyclesMatrixKernel(con, dimension):
    con.query("delete from cycles_matrix")
    con.query("insert into cycles_matrix(col_ix, row_ix, leading_ix, iteration) %s" % CollectKernelQuery())
    assert len(con.query("select *, res_id_is_kernel(row_ix) from cycles_matrix where res_id_is_kernel(row_ix)")) == 0
    assert len(con.query(CheckHomogeneousKernelQuery(dimension))) == 0, con.query(CheckHomogeneousKernelQuery(dimension)).format_table()
    assert len(con.query(CheckCyclesQuery())) == 0, con.query(CheckCyclesQuery()).format_table()

def GenerateNewGenerators(con, grade, dimension):
    con.query("insert into resolution_ids(grade, dimension, from_col_ix) %s" % CollectNewGeneratorIdsQuery(grade, dimension))
    con.query("insert into resolution_generators(id, grade, dimension, differential_gen, differential_square) %s" % CollectNewGeneratorsQuery(grade, dimension))

def E2ASSIteration(con, grade, dimension, use_sp=False):
    if use_sp:
        with Timer("E2ASSIteration(%d,%d)" % (grade, dimension)):
            con.query("call e2_ass_iteration(%d,%d)" % (grade, dimension))
            return
    with Timer("PopulateResolutionMatrix(%d,%d)" % (grade, dimension)):
        PopulateResolutionMatrix(con, grade, dimension)
    with Timer("Triangularize(resolution)(%d,%d)" % (grade, dimension)):
        triangularize.Triangularize(con, "resolution", use_sp=False)
    with Timer("PopulateCyclesMatrixImage(%d,%d)" % (grade, dimension)):
        PopulateCyclesMatrixImage(con, dimension)
    with Timer("Triangularize(cycles)(%d,%d)" % (grade, dimension)):
        triangularize.Triangularize(con, "cycles", use_sp=False)
    with Timer("GenerateNewGenerators(%d,%d)" % (grade, dimension)):
        GenerateNewGenerators(con, grade, dimension)
    with Timer("PopulateCyclesMatrixKernel(%d,%d)" % (grade, dimension)):
        PopulateCyclesMatrixKernel(con, dimension)

def E2ASSGenIterationStoredProc():
    body = ["delete from resolution_matrix;",
            "insert into resolution_matrix(col_ix, row_ix, leading_ix, iteration) %s;" % ResolutionDifferentialQuery("the_grade", "the_dimension"),
            "insert into resolution_matrix(col_ix, row_ix, leading_ix, iteration) %s;" % ResolutionBasisQuery("the_grade", "the_dimension"),

            "call triangularize_resolution();",
            
            "insert into cycles_matrix(col_ix, row_ix, leading_ix, iteration) %s;" % CollectImageQuery(),

            "call triangularize_cycles();",
            
            "insert into resolution_ids(grade, dimension, from_col_ix) %s;" % CollectNewGeneratorIdsQuery("the_grade", "the_dimension"),
            "insert into resolution_generators(id, grade, dimension, differential_gen, differential_square) %s;" % CollectNewGeneratorsQuery("the_grade", "the_dimension"),

            "delete from cycles_matrix;",
            "insert into cycles_matrix(col_ix, row_ix, leading_ix, iteration) %s;" % CollectKernelQuery()]
    return mr_sp.StoredProc("e2_ass_iteration", ["the_grade bigint not null", "the_dimension bigint not null"], None, None, mr_sp.Body(body))

def E2ASSGenGradeStoredProc():
    body = mr_sp.Block("for the_dimension in 1 .. the_grade loop",
                       ["call e2_ass_iteration(the_grade, the_dimension);"],
                       "end loop;")
    return mr_sp.StoredProc("e2_ass_grade", ["the_grade bigint not null"], None, None, mr_sp.Body([body]))

def CreateE2ASSStoredProcs(con):
    for proc in [E2ASSGenIterationStoredProc(), E2ASSGenGradeStoredProc()]:
        print proc.ToSQL()
        con.query(proc.ToSQL())
        print
        
def ClearTables(con):
    con.query("delete from resolution_ids")
    con.query("delete from resolution_generators")
    
def PopulateDimensionZeroGenerator(con):
    ClearTables(con)
    con.query("insert into resolution_ids(grade, dimension, from_col_ix) values (0,0,0)")

def PopulateDimensionZeroKernel(con, grade):
    con.query("delete from cycles_matrix")        
    con.query("""insert into cycles_matrix(col_ix, row_ix, leading_ix, iteration)
                 select
                     res_id_set_kernel(product_to_res_id(resolution_ids.id, sq.id)) as col_ix,
                     product_to_res_id(resolution_ids.id, sq.id) as row_ix,
                     product_to_res_id(resolution_ids.id, sq.id) as leading_ix,
                     0 as iteration
                 from resolution_ids
                 join serre_cartan_elts sq
                 where resolution_ids.dimension = 0
                   and sq.grade = %(grade)s - resolution_ids.grade""" % {
                       "grade" : grade
                   })

def E2ASSGrade(con, grade, use_sp=False):
    with Timer("SteenrodGen(%d)" % (grade + 1)):    
        steenrod_gen.GenForGrade(con, grade + 1, use_sp=use_sp)
    PopulateDimensionZeroKernel(con, grade)
    if use_sp:
        with Timer("E2ASSGrade(%d)" % (grade)):
            con.query("call e2_ass_grade(%d)" % grade)
        return
    for dimension in xrange(1, grade + 1):
        E2ASSIteration(con, grade, dimension, use_sp=False)

def E2ASS(max_grade):
    con = ConnectToMemSQL()
    con.query("set session materialize_ctes='auto'")
    triangularize.CreateStoredProcs(con, "resolution")
    triangularize.CreateStoredProcs(con, "cycles")
    CreateE2ASSStoredProcs(con)
    steenrod_gen.CreateStoredProcs(con)
    PopulateDimensionZeroGenerator(con)
    steenrod_gen.GenForGrade(con, 1, use_sp=not options.no_sp)    
    for grade in xrange(1, max_grade):
        E2ASSGrade(con, grade, use_sp=not options.no_sp)


if __name__ == "__main__":
    E2ASS(10000)
