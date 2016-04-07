import resource
import time
import snap

class Timer(object):
    def __init__(self, enabled = True):
        self.enabled = enabled
        self.time = time.time()
    def show(self, operation, obj = None):
        blanks = (16 - len(operation))*" "
        if self.enabled:
            message = '[%s]%sElapsed: %.2f seconds' % (operation, blanks, time.time() - self.time)
            if not obj is None:
                if isinstance(obj, snap.PTable):
                    message += ', Rows: %d' % obj.GetNumValidRows()
                elif hasattr(obj, 'GetNodes'):
                    # RS, commented out GetEdges() since they are not precomputed
                    #message += ', Nodes: %d, Edges: %d' % (obj.GetNodes(), obj.GetEdges())
                    message += ', Nodes: %d' % (obj.GetNodes())
                elif hasattr(obj, '__len__'):
                    message += ', Length: %d' % len(obj)
            print message
        self.time = time.time()

class Resource(object):
    def __init__(self):
        self.r = resource.getrusage(resource.RUSAGE_SELF)
    def show(self, operation):
        blanks = (16 - len(operation))*" "
        rnow = resource.getrusage(resource.RUSAGE_SELF)
        tnow = rnow.ru_utime + rnow.ru_stime
        mnow = 1.0 * rnow.ru_maxrss / 1000
        tprev = self.r.ru_utime + self.r.ru_stime
        tdiff = tnow - tprev

        print "%s%s\tcpu(s) %.3f\tmem(MB) %.3f" % (operation,blanks,tdiff, mnow)
        self.r = rnow

def dump(table, maxRows = None):
    colSpace = 25
    S = table.GetSchema()
    template = ""
    line = ""
    names = []
    types = []
    for i, attr in enumerate(S):
        template += "{%d: <%d}" % (i, colSpace)
        names.append(attr.GetVal1())
        types.append(attr.GetVal2())
        line += "-" * colSpace
    print template.format(*names)
    print line
    RI = table.BegRI()
    cnt = 0
    while RI < table.EndRI() and (maxRows is None or cnt < maxRows):
        elmts = []
        for c,t in zip(names,types):
            if t == 0: # int
                elmts.append(str(RI.GetIntAttr(c)))
            elif t == 1: # float
                elmts.append("{0:.6f}".format(RI.GetFltAttr(c)))
            elif t == 2: # string
                elmts.append(RI.GetStrAttr(c))
            else:
                raise NotImplementedError("unsupported column type")
        print template.format(*elmts)
        RI.Next()
        cnt += 1

