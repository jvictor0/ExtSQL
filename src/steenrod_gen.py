from memsql import joyo_utils
import math

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
    
def GenSerreCartanBasis(max_grade, start_grade=1):
    con = joyo_utils.ConnectToMemSQL("127.0.0.1:10000", database="ext_sql")
    con.query("delete from serre_cartan_elts where grade >= %d" % start_grade)
    for i in xrange(start_grade, max_grade + 1):
        print "generating basis in grade = %d" % (i)
        InsertEltsInGrade(con, i)

def Choose(n,k):
    if n < 0 or k < 0 or k > n:
        return 0
    a = math.factorial(n)
    b = math.factorial(k)
    c = math.factorial(n - k)
    return a / (b * c)    
        
def GenBinomial(max_grade):
    con = joyo_utils.ConnectToMemSQL("127.0.0.1:10000", database="ext_sql")
    con.query("delete from nonzero_binomial_coefs")
    print "generating binomial coefs"
    for j in xrange(1, max_grade + 1):
        rows = []
        for i in xrange(1, 2 * j):
            for k in xrange(i / 2 + 1):
                if Choose(j - k - 1, i - 2 * k) % 2 == 1:
                    rows.append((i,j,k))
        if len(rows) > 0:
            con.query("insert into nonzero_binomial_coefs(i,j,k) values " + ",".join([str(t) for t in rows]))

def GenProductsSingletonLHS(max_grade):
    con = joyo_utils.ConnectToMemSQL("127.0.0.1:10000", database="ext_sql")
    con.query("delete from steenrod_products")

    # Grab all ring generators (that is, products of length one) from the serre cartan table
    #
    ring_generators = {int(r["leading_square"]) : int(r["id"])
                       for r in con.query("select leading_square, id from serre_cartan_elts where length(squares) = 2")}

    for grade in xrange(1, max_grade + 1):
        print "generating multiplication table in grade %d" % grade
        # The assumtion here is that all products with higher degree LHS have already been computed.
        # This way, the Adem relations will give us things we've already computed so we can just look them up rather than recursing
        #
        for square in xrange(grade - 1, 0, -1):
            # Generate all trivial products, that is, all products that are obviously Serre-Cartan
            #
            con.query("""insert into steenrod_products 
                         (lhs_id, lhs_squares, lhs_grade,
                          rhs_id, rhs_squares, rhs_grade,
                          prod_id, prod_squares)
                          select 
                              %(square_id)d as lhs_id,
                              number_to_two_byte_blob(%(square)d) as lhs_squares,
                              %(square)d as lhs_grade,
                              rhs.id as rhs_id,
                              rhs.squares as rhs_squares,
                              rhs.grade as rhs_grade,
                              prod.id as prod_id,
                              prod.squares as prod_squares
                          from serre_cartan_elts rhs join serre_cartan_elts prod
                          on concat(number_to_two_byte_blob(%(square)d), rhs.squares) = prod.squares
                          where rhs.grade = %(rhs_grade)d
                            and prod.grade = %(prod_grade)d
                            and rhs.leading_square <= %(leading_square_min)d"""
                      % {"square_id" : ring_generators[square],
                         "square" : square,
                         "rhs_grade" : grade - square,
                         "prod_grade" : grade,
                         "leading_square_min" : square / 2})

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
                             %(square_id)d as lhs_id,
                             number_to_two_byte_blob(%(square)d) as lhs_squares,
                             %(square)d as lhs_grade,
                             rhs.id as rhs_id,
                             rhs.squares as rhs_squares,
                             rhs.grade as rhs_grade,
                             steenrod_products.prod_id as prod_id,
                             steenrod_products.prod_squares as prod_squares
                         from serre_cartan_elts rhs
                         join nonzero_binomial_coefs
                           on i = %(square)d
                          and j = rhs.leading_square
                         join steenrod_products
                           on i + j - k = lhs_grade                          
                          and length(lhs_trailing_squares) = 0
                          and ((k = 0 and rhs_squares = rhs.trailing_squares)
                               or (rhs_leading_square = k
                                   and rhs_trailing_squares = rhs.trailing_squares))
                         where rhs.leading_square > %(leading_square_min)d
                           and rhs.grade = %(rhs_grade)d"""
                      % {"square_id" : ring_generators[square],
                         "square" : square,
                         "rhs_grade" : grade - square,
                         "prod_grade" : grade,
                         "leading_square_min" : square / 2})
            con.query(query)
            
def GenAll(max_grade):
    GenSerreCartanBasis(max_grade)
    GenBinomial(max_grade)
    GenProductsSingletonLHS(max_grade)
     
def MultToy(sq1, sq2):
    con = joyo_utils.ConnectToMemSQL("127.0.0.1:10000", database="ext_sql")
    rows = con.query("""select *
                        from steenrod_products
                        where lhs_squares = concat(%s) and rhs_squares = concat(%s)"""
                     % (",".join(map(TwoByteHex, sq1)), ",".join(map(TwoByteHex, sq2))))
    return [SquareFromBlob(r["prod_squares"]) for r in rows]
