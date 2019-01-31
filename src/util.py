from memsql import joyo_utils

def Indent(string, amount=4):
    string  = string.strip()
    string = "\n" + string
    return string.replace("\n", "\n" + (" " * amount)) + "\n"
    
def Dedent(string):
    amount = 0
    while len(string) > 0 and string[0] == "\n":
        string = string[1:]
    while len(string) > amount and string[amount] == " ":
        amount += 1
    return string.strip().replace("\n" + (" " * amount), "\n")

def ConnectToMemSQL(use_db=True):
    return joyo_utils.ConnectToMemSQL("127.0.0.1:10000", database="ext_sql" if use_db else "")

def Log(msg):
    print msg
