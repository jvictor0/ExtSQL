from util import *
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

def E2ASSIteration(con, grade, dimension):
    Log("Doing iteration %d, %d" % (grade, dimension))
    PopulateResolutionMatrix(con, grade, dimension)
    print con.query("select count(distinct col_ix) as vec_size from resolution_matrix").format_table()
    triangularize.Triangularize(con, "resolution")
    print con.query("select count(distinct col_ix) as vec_size_post from resolution_matrix").format_table()    
    print con.query("select count(distinct col_ix) as cyc_size_pre, count(distinct row_ix) from cycles_matrix").format_table()    
    PopulateCyclesMatrixImage(con, dimension)
    print con.query("select count(distinct col_ix) as cyc_size, count(distinct row_ix) from cycles_matrix").format_table()
    triangularize.Triangularize(con, "cycles")
    print con.query("select count(distinct col_ix) as cyc_size_post, count(distinct row_ix) from cycles_matrix").format_table()
    GenerateNewGenerators(con, grade, dimension)
    PopulateCyclesMatrixKernel(con, dimension)
    print con.query("select count(distinct col_ix) as kern_size, count(distinct row_ix) from cycles_matrix").format_table()
    
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

def E2ASSGrade(con, grade):
    steenrod_gen.GenForGrade(con, grade + 1)
    PopulateDimensionZeroKernel(con, grade)
    for dimension in xrange(1, grade + 1):
        E2ASSIteration(con, grade, dimension)

def E2ASS(max_grade):
    con = ConnectToMemSQL()
    PopulateDimensionZeroGenerator(con)
    for grade in xrange(1, max_grade):
        E2ASSGrade(con, grade)

