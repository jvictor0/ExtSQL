import util

def ToSQL(s):
    if isinstance(s, str):
        return s
    return s.ToSQL()

class Block(object):
    def __init__(self, header, body, footer):
        self.header = header
        self.body = body
        self.footer = footer

    def ToSQL(self):
        result = self.header + "\n"
        for b in self.body:
            result += util.Indent(ToSQL(b))
        if self.footer is not None:
            result += "\n" + self.footer
        return result

class Body(Block):
    def __init__(self, body):
        super(Body, self).__init__("begin", body, "end")

class Declare(Block):
    def __init__(self, body):
        super(Declare, self).__init__("declare", body, None)
        
class While(Block):
    def __init__(self, cond, body):
        super(While, self).__init__("while %s loop" % cond, body, "end loop;")

class If(Block):
    def __init__(self, cond, body):
        super(If, self).__init__("if %s then" % cond, body, "end if;")
        
class StoredProc:
    def __init__(self, name, args, returns, declare, body):
        self.name = name
        self.args = args
        self.returns = returns
        self.declare = declare
        self.body = body

    def ToSQL(self):
        result = ""
        result += "create or replace procedure " + self.name + " ("
        for ix, a in enumerate(self.args):
            result += "\n    " + a
            if ix < len(self.args) - 1:
                result += ","
        result += ")"
        if self.returns is not None:
            result += " returns " + self.returns
        result += "\nas\n"
        if self.declare is not None:
            result += self.declare.ToSQL() + "\n"
        result += self.body.ToSQL()
        return result
        
