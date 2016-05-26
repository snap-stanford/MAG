
# coding: utf-8

# In[1]:

import sys
import os
import time
import snap
print snap.Version
import testutils

DEBUG = False # Set this to true if you just want to run the code and print the first few lines of each part of the output.


context = snap.TTableContext()

t = testutils.Timer()
r = testutils.Resource()

# The source file where we dumped the binaries of all the tables
srcfile = "/dfs/scratch0/viswa/fromlfs/mag-022016-papers-paa-authors-affls-refs-context-net.bin"
FIn = snap.TFIn(srcfile)

# load papers
print time.ctime(), "loading papers ..."
PapT = snap.TTable.Load(FIn,context)
t.show("loadbin papers", PapT)
r.show("__papers__")

# Load paper author affiliation table
print time.ctime(), "loading paa ..."
PapAuthAfflT = snap.TTable.Load(FIn,context)
t.show("loadbin paa", PapAuthAfflT)
r.show("__paa__")

# Load authors table
print time.ctime(), "loading auth ..."
AuthT = snap.TTable.Load(FIn,context)
t.show("loadbin auth", AuthT)
r.show("__auth__")

# Load affiliations table
print time.ctime(), "loading affl ..."
AfflT = snap.TTable.Load(FIn,context)
t.show("loadbin affl", AfflT)
r.show("__affl__")

# load references
print time.ctime(), "loading references ..."
RefsT = snap.TTable.Load(FIn,context)
t.show("loadbin references", RefsT)
r.show("__references__")

# load context
print time.ctime(), "loading context ..."
context.Load(FIn)
t.show("loadbin context", RefsT)
r.show("__context__")

print time.ctime(), "done"


# In[2]:

mmnet = snap.TMMNet.New()


# In[3]:

nodeattrv = snap.TStrV()
print time.ctime()
snap.LoadModeNetToNet(mmnet, "papers", PapT, "PaperID", nodeattrv)
print time.ctime()


# In[6]:

print time.ctime()
snap.LoadModeNetToNet(mmnet, "authors", AuthT, "AuthorID", nodeattrv)
print time.ctime()


# In[4]:

print time.ctime()
snap.LoadModeNetToNet(mmnet, "affls", AfflT, "AfflID", nodeattrv)
print time.ctime()


# In[ ]:

edgeattrv = snap.TStrV()
print time.ctime()
snap.LoadCrossNetToNet(mmnet, "papers", "authors", "papauth", PapAuthAfflT, "PaperID", "AuthorID", edgeattrv)
print time.ctime()


# In[5]:

edgeattrv = snap.TStrV()
print time.ctime()
snap.LoadCrossNetToNet(mmnet, "papers", "affls", "papaffl", PapAuthAfflT, "PaperID", "AfflID", edgeattrv)
print time.ctime()


# In[ ]:

print time.ctime()
snap.LoadCrossNetToNet(mmnet, "authors", "affls", "authaffl", PapAuthAfflT, "AuthorID", "AfflID", edgeattrv)
print time.ctime()


# In[ ]:

print time.ctime()
refs_schema = map(lambda x: x.GetVal1(), RefsT.GetSchema())
snap.LoadCrossNetToNet(mmnet, "papers", "papers", "refs", RefsT, refs_schema[0], refs_schema[1], edgeattrv)
print time.ctime()


# In[ ]:

for modename in ["papers", "authors", "affls"]:
    modenet = mmnet.GetModeNetByName(modename)
    print modenet.GetNodes()


# In[ ]:

for crossnetname in ["refs", "papauth", "authaffl", "papaffl"]:
    crossnet = mmnet.GetCrossNetByName(crossnetname)
    print crossnetname, crossnet.GetEdges()


# In[ ]:

crossnetnames = snap.TStrV()
crossnetnames.Add("refs")
print time.ctime()
refs_pneanet = mmnet.ToNetworkMP(crossnetnames)
print time.ctime()

snap.PlotInDegDistr(refs_pneanet, "refs_indeg", "References graph - in-degree Distribution")

print time.ctime()
snap.PlotOutDegDistr(refs_pneanet, "refs_outdeg", "References graph - out-degree distribution")
print time.ctime()
