# In[1]:

import sys
import os
import time
sys.path.append("../utils")
import snap
import testutils

context = snap.TTableContext()

# In[2]:

print time.ctime()
t = testutils.Timer()
r = testutils.Resource()

# In[3]:

# load paper table
paperfile = "/dfs/scratch0/viswa/mag022016/Papers.txt"
schema = snap.Schema()
schema.Add(snap.TStrTAttrPr("PaperID", snap.atStr))
schema.Add(snap.TStrTAttrPr("PaperTitle", snap.atStr))
schema.Add(snap.TStrTAttrPr("PaperTitleNorm", snap.atStr))
schema.Add(snap.TStrTAttrPr("PaperYear", snap.atStr))
schema.Add(snap.TStrTAttrPr("PaperDate", snap.atStr))
schema.Add(snap.TStrTAttrPr("PaperDOI", snap.atStr))
schema.Add(snap.TStrTAttrPr("VenueName", snap.atStr))
schema.Add(snap.TStrTAttrPr("VenueNameNorm", snap.atStr))
schema.Add(snap.TStrTAttrPr("JournalID", snap.atStr))
schema.Add(snap.TStrTAttrPr("SeriesID", snap.atStr))
schema.Add(snap.TStrTAttrPr("PaperRank", snap.atInt))
papers = snap.TTable.LoadSS(schema, paperfile, context, "\t", snap.TBool(False))
print time.ctime()
t.show("load text", papers)
r.show("__loadtext__")

# In[4]:
paafile = "/dfs/scratch0/viswa/mag022016/PaperAuthorAffiliations.txt"
paaschema = snap.Schema()
paaschema.Add(snap.TStrTAttrPr("PaperID", snap.atStr))
paaschema.Add(snap.TStrTAttrPr("AuthorID", snap.atStr))
paaschema.Add(snap.TStrTAttrPr("AfflID", snap.atStr))
paaschema.Add(snap.TStrTAttrPr("AfflName", snap.atStr))
paaschema.Add(snap.TStrTAttrPr("AfflNameNorm", snap.atStr))
paaschema.Add(snap.TStrTAttrPr("AuthSeqNum", snap.atInt))
paa = snap.TTable.LoadSS(paaschema, paafile, context, "\t", snap.TBool(False))
print time.ctime()
t.show("load paa", paa)
r.show("__loadpaa__")

authorfile = "/dfs/scratch0/viswa/mag022016/Authors.txt"
authschema = snap.Schema()
authschema.Add(snap.TStrTAttrPr("AuthorID", snap.atStr))
authschema.Add(snap.TStrTAttrPr("AuthorName", snap.atStr))
auth = snap.TTable.LoadSS(authschema, authorfile, context, "\t", snap.TBool(False))
print time.ctime()
t.show("load auth", auth)
r.show("__loadauth__")

afflfile = "/dfs/scratch0/viswa/mag022016/Affiliations.txt"
afflschema = snap.Schema()
afflschema.Add(snap.TStrTAttrPr("AfflID", snap.atStr))
afflschema.Add(snap.TStrTAttrPr("AfflName", snap.atStr))
affl = snap.TTable.LoadSS(afflschema, afflfile, context, "\t", snap.TBool(False))
print time.ctime()
t.show("load affl", affl)
r.show("__loadaffl__")


# load references table
reffile = "/dfs/scratch0/viswa/mag022016/PaperReferences.txt"
schema = snap.Schema()
schema.Add(snap.TStrTAttrPr("PaperID", snap.atStr))
schema.Add(snap.TStrTAttrPr("RefID", snap.atStr))
refs = snap.TTable.LoadSS(schema, reffile, context, "\t", snap.TBool(False))
print time.ctime()
t.show("load text", refs)
r.show("__loadtext__")

# In[5]:

# create network
schema = map(lambda x: x.GetVal1(), refs.GetSchema())
net = snap.ToGraph(snap.PNGraph, refs, schema[0], schema[1], snap.aaFirst)
print time.ctime()
t.show("create network", net)
r.show("__network__")

# In[6]:

# save tables and context

dstfile = "mag-022016-papers-paa-authors-affls-refs-context-graph.bin"
FOut = snap.TFOut(dstfile)

# save papers
papers.Save(FOut)
FOut.Flush()
print time.ctime()
t.show("save papers", papers)
r.show("__savebin__")

paa.Save(FOut)
FOut.Flush()
print time.ctime()
t.show("save paa", paa)
r.show("__savebin__")

auth.Save(FOut)
FOut.Flush()
print time.ctime()
t.show("save auth", auth)
r.show("__savebin__")

affl.Save(FOut)
FOut.Flush()
print time.ctime()
t.show("save affl", affl)
r.show("__savebin__")

# save references
refs.Save(FOut)
FOut.Flush()
print time.ctime()
t.show("save refs", refs)
r.show("__savebin__")

# save context
context.Save(FOut)
FOut.Flush()
print time.ctime()
t.show("save context", refs)
r.show("__savebin__")

# save network
net.Save(FOut)
FOut.Flush()
print time.ctime()
t.show("save net", net)
r.show("__savebin__")

