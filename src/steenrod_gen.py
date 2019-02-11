from util import *
import math
import time

def TwoByteHex(num):
    if num < 256:
        return '0x00' + hex(num)[2:]
    else:
        return hex(num)

def SquareFromBlob(blob):
    result = []
    for i in xrange(len(blob) / 2):
        result.append(ord(blob[2*i]) << 8 | ord(blob[2*i+1]))
    return result
    
def InsertEltsInGrade(con, grade):
    con.query("insert into serre_cartan_elts (squares, grade) values (number_to_two_byte_blob(%d), %d)" % (grade, grade))
    con.query("""insert into serre_cartan_elts (squares, grade) 
                 select 
                 concat(number_to_two_byte_blob(%(grade)d - grade), squares) as squares, 
                 %(grade)d as grade
                 from serre_cartan_elts
                 where 2 * leading_square <= (%(grade)d - grade)"""
              % {"grade": grade, "max_grade": 2 * grade / 3 - 1})
    
def GenSerreCartanBasis(con, grade):
    con.query("delete from serre_cartan_elts where grade >= %d" % grade)
    print "generating basis in grade = %d" % grade
    InsertEltsInGrade(con, grade)

def Choose(n,k):
    if n < 0 or k < 0 or k > n:
        return 0
    a = math.factorial(n)
    b = math.factorial(k)
    c = math.factorial(n - k)
    return a / (b * c)    
        
def GenBinomial(con, j):
    con.query("delete from nonzero_binomial_coefs where j >= %d" % j)
    print "generating binomial coefs"
    rows = []

    ring_generators = {int(r["leading_square"]) : int(r["id"])
                       for r in con.query("select leading_square, id from serre_cartan_elts where length(squares) = 2")}
    ring_generators[0] = 0

    for i in xrange(1, 2 * j):
        for k in xrange(i / 2 + 1):
            if Choose(j - k - 1, i - 2 * k) % 2 == 1:
                rows.append((i,j,k, ring_generators[i+j-k], ring_generators[k]))
    if len(rows) > 0:
        con.query("insert into nonzero_binomial_coefs(i,j,k,i_plus_j_minus_k_square,k_square) values " + ",".join([str(t) for t in rows]))

def GenProductsSingletonLHS(con, grade):
    con.query("delete from steenrod_products where prod_grade >= %d" % grade)

    # Grab all ring generators (that is, products of length one) from the serre cartan table
    #
    ring_generators = {int(r["leading_square"]) : int(r["id"])
                       for r in con.query("select leading_square, id from serre_cartan_elts where length(squares) = 2")}

    
    print "generating multiplication table in grade %d" % grade
    # The assumtion here is that all products with higher degree LHS have already been computed.
    # This way, the Adem relations will give us things we've already computed so we can just look them up rather than recursing
    #
    # Generate all trivial products, that is, all products that are obviously Serre-Cartan
    #
    query = ("""insert into steenrod_products 
                 (lhs_id, lhs_squares, lhs_grade,
                  rhs_id, rhs_squares, rhs_grade,
                  prod_id, prod_squares)
                  select 
                      lhs.id as lhs_id,
                      lhs.squares as lhs_squares,
                      lhs.grade as lhs_grade,
                      rhs.id as rhs_id,
                      rhs.squares as rhs_squares,
                      rhs.grade as rhs_grade,
                      prod.id as prod_id,
                      prod.squares as prod_squares
                  from serre_cartan_elts lhs
                  join serre_cartan_elts rhs 
                  join serre_cartan_elts prod
                  on concat(lhs.squares, rhs.squares) = prod.squares
                  where lhs.grade < %(grade)d
                    and lhs.trailing_squares = ''                        
                    and rhs.grade = %(grade)d - lhs.grade
                    and prod.grade = %(grade)d
                    and rhs.leading_square <= floor(lhs.grade / 2)"""
              % {"grade" : grade})
    t0 = time.time()
    con.query(query)
    print "   query 4 took %f secs" % (time.time() - t0)

    for square in xrange(grade - 1, 0, -1):
        # Insert the product of two primitive squares being primitive
        #
        if Choose(grade - square - 1, square) % 2 == 1:
            con.query("""insert into steenrod_products 
                     (lhs_id, lhs_squares, lhs_grade,
                      rhs_id, rhs_squares, rhs_grade,
                      prod_id, prod_squares)
                      values 
                      (%d, number_to_two_byte_blob(%d), %d,
                       %d, number_to_two_byte_blob(%d), %d,
                       %d, number_to_two_byte_blob(%d))"""
                      % (ring_generators[square], square, square,
                         ring_generators[grade - square], grade - square, grade - square,
                         ring_generators[grade], grade))

    # Use the Adem relations to fill in the rest
    #
    query = ("""insert into steenrod_products
                 (lhs_id, lhs_squares, lhs_grade,
                  rhs_id, rhs_squares, rhs_grade,
                  prod_id, prod_squares)
                 select
                     lhs.id as lhs_id,
                     lhs.squares as lhs_squares,
                     lhs.grade as lhs_grade,
                     rhs.id as rhs_id,
                     rhs.squares as rhs_squares,
                     rhs.grade as rhs_grade,
                     steenrod_products.prod_id as prod_id,
                     steenrod_products.prod_squares as prod_squares
                 from serre_cartan_elts lhs
                 join serre_cartan_elts rhs
                 join nonzero_binomial_coefs
                   on i = lhs.grade
                  and j = rhs.leading_square
                 join steenrod_products
                   on i + j - k = steenrod_products.lhs_grade
                  and i_plus_j_minus_k_square = steenrod_products.lhs_id
                  and %(grade)d - (i + j - k) = steenrod_products.rhs_grade
                  and ((k = 0 and steenrod_products.rhs_squares = rhs.trailing_squares)
                       or (steenrod_products.rhs_leading_square = k
                           and rhs_trailing_squares = rhs.trailing_squares))
                 where lhs.trailing_squares = ''
                   and lhs.grade < %(grade)d
                   and rhs.leading_square > floor(lhs.grade / 2)
                   and rhs.grade = %(grade)d - lhs.grade"""
              % {"grade" : grade})
    t0 = time.time()
    con.query(query)
    print "   query 4 took %f secs" % (time.time() - t0)

    # Annoyingly, the rhs may be a product...
    #
    query = ("""insert into steenrod_products
                 (lhs_id, lhs_squares, lhs_grade,
                  rhs_id, rhs_squares, rhs_grade,
                  prod_id, prod_squares)
                 select
                     lhs.id as lhs_id,
                     lhs.squares as lhs_squares,
                     lhs.grade as lhs_grade,
                     rhs.id as rhs_id,
                     rhs.squares as rhs_squares,
                     rhs.grade as rhs_grade,
                     steenrod_products.prod_id as prod_id,
                     steenrod_products.prod_squares as prod_squares
                 from serre_cartan_elts lhs
                 join serre_cartan_elts rhs
                 join nonzero_binomial_coefs
                   on i = lhs.grade
                  and j = rhs.leading_square
                 join steenrod_products rhs_product
                   on rhs_product.lhs_grade = k
                  and rhs_product.lhs_id = k_square
                  and rhs_grade = rhs.grade - rhs.leading_square
                  and rhs_product.rhs_squares = rhs.trailing_squares
                  and not rhs_product.is_trivial
                 join steenrod_products
                   on i + j - k = steenrod_products.lhs_grade                          
                  and i_plus_j_minus_k_square = steenrod_products.lhs_id
                  and steenrod_products.rhs_id = rhs_product.prod_id
                 where lhs.trailing_squares = ''
                   and lhs.grade < %(grade)d
                   and rhs.leading_square > floor(lhs.grade / 2)
                   and rhs.grade = %(grade)d - lhs.grade"""
              % {"grade" : grade})
    t0 = time.time()
    con.query(query)
    print "   query 4 took %f secs" % (time.time() - t0)
         
            
def GenProductsExtendLHSOnce(con, lhs_length, grade):
    con.query("delete from steenrod_products where length(lhs_squares) / 2 >= %d and prod_grade >= %d" % (lhs_length, grade))
    query = ("""insert into steenrod_products
                 (lhs_id, lhs_squares, lhs_grade,
                  rhs_id, rhs_squares, rhs_grade,
                  prod_id, prod_squares)
                 select 
                     lhs.id as lhs_id,
                     lhs.squares as lhs_squares,
                     lhs.grade as lhs_grade,
                     pre_product.rhs_id as rhs_id,
                     pre_product.rhs_squares as rhs_squares,
                     pre_product.rhs_grade as rhs_grade,
                     final_product.prod_id as prod_id,
                     final_product.prod_squares as prod_squares
                 from serre_cartan_elts lhs
                 join steenrod_products pre_product
                   on pre_product.lhs_squares = lhs.trailing_squares
                 join steenrod_products final_product
                   on final_product.lhs_leading_square = lhs.leading_square
                  and final_product.lhs_trailing_squares = ''
                  and final_product.rhs_id = pre_product.prod_id
                 where length(lhs.squares) = 2 * %(lhs_length)d
                  and final_product.prod_grade = %(grade)d
                 group by lhs.id, pre_product.rhs_id, final_product.prod_id
                 having count(*) %% 2 = 1""" % {
                     "lhs_length" : lhs_length,
                     "grade": grade})
    return con.query(query)
    
    
def GenProductsExtendLHS(con, grade):
    starting_length = 2
    while True:
        print "Extending LHS to lenght %d" % starting_length
        result = GenProductsExtendLHSOnce(con, starting_length, grade)
        starting_length += 1
        if result == 0:
            break

# The invariant this upholds is that when its over, we have all products up to grade
# Thus we need to generate twice as many singleton products so we can generate the longer products
#
def GenForGrade(con, grade):
    for i in xrange(4):
        GenSerreCartanBasis(con, 4 * grade - 3 + i)
    GenBinomial(con, 2 * grade - 1)
    GenBinomial(con, 2 * grade)
    GenProductsSingletonLHS(con, 2 * grade - 1)
    GenProductsSingletonLHS(con, 2 * grade)
    GenProductsExtendLHS(con, grade)
        
def GenAll(max_grade):
    con = ConnectToMemSQL()
    for grade in xrange(1, max_grade + 1):
        GenForGrade(con, grade)
        
def MultToy(sq1, sq2):
    con = ConnectToMemSQL()
    rows = con.query("""select *
                        from steenrod_products
                        where lhs_squares = concat(%s) and rhs_squares = concat(%s)"""
                     % (",".join(map(TwoByteHex, sq1)), ",".join(map(TwoByteHex, sq2))))
    return [SquareFromBlob(r["prod_squares"]) for r in rows]
