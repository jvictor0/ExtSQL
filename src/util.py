from memsql import joyo_utils
import optparse
import time

parser = optparse.OptionParser()
parser.add_option("--host", type='string', default='127.0.0.1:10000')
parser.add_option("--user", type='string', default='root')
parser.add_option("--password", type='string', default=None)
parser.add_option("--no-sp", default=False, action='store_true')
options = parser.parse_args()[0]

def Indent(string, amount=4, tail='\n'):
    string  = string.strip()
    string = "\n" + string
    return string.replace("\n", "\n" + (" " * amount)) + tail
    
def Dedent(string):
    amount = 0
    while len(string) > 0 and string[0] == "\n":
        string = string[1:]
    while len(string) > amount and string[amount] == " ":
        amount += 1
    return string.strip().replace("\n" + (" " * amount), "\n")

def ConnectToMemSQL(use_db=True):
    return joyo_utils.ConnectToMemSQL(options.host, database="ext_sql" if use_db else "", password=options.password, user=options.user)

def Log(msg):
    print msg

class Timer:
    def __init__(self, msg):
        self.msg = msg
        self.start = time.time()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, exc_traceback):
        Log("Operation %s took %f seconds" % (self.msg, time.time() - self.start))
