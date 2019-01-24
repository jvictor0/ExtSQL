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
