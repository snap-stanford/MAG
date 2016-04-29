
# coding: utf-8

# In[1]:

import sys
import os
import time
sys.path.append("../utils")
import snap
print snap.Version
import testutils

context = snap.TTableContext()

t = testutils.Timer()
r = testutils.Resource()

# load tables and context
srcfile = "mag-022016-papers-paa-authors-affls-refs-context-net.bin"
FIn = snap.TFIn(srcfile)

# load papers
print time.ctime(), "loading papers ..."
papers = snap.TTable.Load(FIn,context)
t.show("loadbin papers", papers)
r.show("__papers__")

print time.ctime(), "loading paa ..."
paa = snap.TTable.Load(FIn,context)
t.show("loadbin paa", paa)
r.show("__paa__")

print time.ctime(), "loading auth ..."
auth = snap.TTable.Load(FIn,context)
t.show("loadbin auth", auth)
r.show("__auth__")

print time.ctime(), "loading affl ..."
affl = snap.TTable.Load(FIn,context)
t.show("loadbin affl", affl)
r.show("__affl__")

# load references
print time.ctime(), "loading references ..."
refs = snap.TTable.Load(FIn,context)
t.show("loadbin references", refs)
r.show("__references__")

# load context
print time.ctime(), "loading context ..."
context.Load(FIn)
t.show("loadbin context", refs)
r.show("__context__")

# load network
print time.ctime(), "loading network ..."
net = snap.TNEANet.Load(FIn)
t.show("loadbin network", net)
r.show("__network__")

print time.ctime(), "done"


# In[2]:

print time.ctime(), "computing indegv ..."
InDegV = snap.TIntPrV()
snap.GetNodeInDegV(net, InDegV)
t.show("indegv", InDegV)
r.show("__InDegV__")
print time.ctime()


# In[3]:

print time.ctime(), "computing pagerank ..."
PRankH = snap.TIntFltH()
snap.GetPageRankMP(net, PRankH, 0.85, 1e-4, 100)
t.show("prank", PRankH)
r.show("__PRankH__")
print time.ctime()


# In[4]:

print time.ctime(), "creating TTable with PageRank ..."
PRankT = snap.TTable(PRankH, "PaperID", "PageRankScore", context, snap.TBool(True))
t.show("prankt", PRankT)
r.show("__PRankT__")
print time.ctime()


# In[5]:

print time.ctime(), "Creating table after join ..."
PaperPRankJoinT = papers.Join("PaperID", PRankT, "PaperID")
t.show("paperprankjoint", PaperPRankJoinT)
r.show("__PaperPRankJoinT__")
print time.ctime()


# In[6]:

print time.ctime()
orderby = snap.TStrV()
orderby.Add("PageRankScore")
PaperPRankJoinT.Order(orderby, "", snap.TBool(False), snap.TBool(False))
print time.ctime()


# In[14]:

print time.ctime()
InDegH = snap.TIntH()
i = 0
for item in InDegV:
    i += 1
    if i % 1000000 == 0:
        print i
    InDegH.AddDat(item.GetVal1(), item.GetVal2())
#InDegT = snap.TTable(PRankH, "PaperID", "PageRankScore", context, snap.TBool(True))
print time.ctime()


# In[15]:

print time.ctime()
InDegT = snap.TTable(InDegH, "PaperID", "InDeg", context, snap.TBool(True))
print time.ctime()


# In[16]:

print time.ctime(), "Creating table after join ..."
PaperInDegJoinT = papers.Join("PaperID", InDegT, "PaperID")
print time.ctime()


# In[17]:

print time.ctime()
orderby = snap.TStrV()
orderby.Add("InDeg")
PaperInDegJoinT.Order(orderby, "", snap.TBool(False), snap.TBool(False))
print time.ctime()


# In[7]:

paa_rit = paa.BegRI()
schema = paa.GetSchema()
#for i in range(schema.Len()):
#    print schema[i].GetVal1(), schema[i].GetVal2()
i = 0
print dir(paa)
while paa_rit < paa.EndRI():
    print paa_rit.GetRowIdx(), paa_rit.GetStrMapByName("PaperID"), context.GetStr(paa_rit.GetStrMapByName("PaperID"))
    print paa.GetStrVal("PaperID", paa_rit.GetRowIdx())
    paa_rit.Next()
    print 'Next'
    i += 1
    if i > 4:
        break


# In[47]:

r_it = PaperPRankJoinT.BegRI()
schema = PaperPRankJoinT.GetSchema()
num_cols = schema.Len()
for i in range(num_cols):
    print schema[i].GetVal1(), schema[i].GetVal2()
for i in range(10):
    print r_it.GetRowIdx(), context.GetStr(r_it.GetStrMapByName("PaperTitle")), r_it.GetFltAttr("PageRankScore"),
    r_it.Next()
    i += 1
    if i > 4:
        break


# In[ ]:




# In[ ]:



