
# coding: utf-8

# In[83]:

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
t.show("loadbin context", refs)
r.show("__context__")

print time.ctime(), "done"


# In[2]:

# Create the network
refs_schema = map(lambda x: x.GetVal1(), refs.GetSchema())
print time.ctime(), "Creating network ..."
net = snap.ToGraph(snap.PNGraph, RefsT, refs_schema[0], refs_schema[1], snap.aaFirst)
print time.ctime(), "done."


# In[3]:

# Compute InDegV from references graph to get number of citations of each paper.
print time.ctime(), "Computing indegv ..."
InDegV = snap.TIntPrV()
snap.GetNodeInDegV(net, InDegV)
t.show("indegv", InDegV)
r.show("__InDegV__")
print time.ctime()


# In[4]:

# Compute PageRank from references graph.
print time.ctime(), "Computing pagerank ..."
PRankH = snap.TIntFltH()
snap.GetPageRankMP(net, PRankH, 0.85, 1e-4, 100)
t.show("prank", PRankH)
r.show("__PRankH__")
print time.ctime()


# In[5]:

# Create a TTable with PageRank
print time.ctime(), "Creating TTable with PageRank ..."
PRankT = snap.TTable(PRankH, "PaperID", "PageRankScore", context, snap.TBool(True))
t.show("prankt", PRankT)
r.show("__PRankT__")
print time.ctime()


# In[ ]:

# Moving InDegV into a HashTable to construct a TTable
print time.ctime(), "Creating TTable with InDegV ..."
InDegH = snap.TIntH()
for item in InDegV:
    InDegH.AddDat(item.GetVal1(), item.GetVal2())
print time.ctime()
InDegT = snap.TTable(InDegH, "PaperID", "InDeg", context, snap.TBool(True))
print time.ctime()


# In[84]:

#Joining papers, paperauthaffls, affls and auths into a giant table.
print time.ctime(), "Joining Paper/Auth/Affl Tables ..."
PapAAJoinT = PapT.Join("PaperID", PapAuthAfflT, "PaperID")
print time.ctime()
PapAA_AJoinT = PapAAJoinT.Join("AfflID", AfflT, "AfflID")
print time.ctime()
PaperAA_AAJoinT = PaperAA_AJoinT.Join("AuthorID", AuthT, "AuthorID")
print time.ctime()


# In[85]:

print time.ctime(), "Joining big table with PageRank and InDeg tables ..."
PaperAA_PRJoinT = PaperAA_AAJoinT.Join("PaperID-1", PRankT, "PaperID")
print time.ctime()
PaperAA_PRIDJoinT = InDegT.Join("PaperID-1", PaperAA_PRJoinT, "PaperID")
print time.ctime()


# In[86]:

# Now, we aggregate along different columns.
print time.ctime(), "Aggregating Affl PageRank ..."
GroupBy = snap.TStrV()
GroupBy.Add("AfflID-1")
PaperAA_PRIDJoinT.Aggregate(GroupBy, snap.aaSum, "PageRankScore", "PRScoreAggAffl", snap.TBool(False))
print time.ctime()

print time.ctime(), "Aggregating Affl InDeg ..."
GroupBy = snap.TStrV()
GroupBy.Add("AfflID-1")
PaperAA_PRIDJoinT.Aggregate(GroupBy, snap.aaSum, "InDeg", "InDegAggAffl", snap.TBool(False))
print time.ctime()

print time.ctime(), "Aggregating Auth PageRank"
GroupBy = snap.TStrV()
GroupBy.Add("AuthorID-1")
PaperAA_PRIDJoinT.Aggregate(GroupBy, snap.aaSum, "PageRankScore", "PRScoreAggAuth", snap.TBool(False))
print time.ctime()

print time.ctime(), "Aggregating Auth InDeg ..."
GroupBy = snap.TStrV()
GroupBy.Add("AuthorID-1")
PaperAA_PRIDJoinT.Aggregate(GroupBy, snap.aaSum, "InDeg", "InDegAggAuth", snap.TBool(False))
print time.ctime()

print time.ctime(), "Counting number of papers by author ..."
GroupBy = snap.TStrV()
GroupBy.Add("AuthorID-1")
PaperAA_PRIDJoinT.Aggregate(GroupBy, snap.aaCount, "PageRankScore", "CountAggAuth", snap.TBool(False))
print time.ctime()

print time.ctime(), "Counting number of papers by affiliation ..."
GroupBy = snap.TStrV()
GroupBy.Add("AfflID-1")
PaperAA_PRIDJoinT.Aggregate(GroupBy, snap.aaCount, "PageRankScore", "CountAggAffl", snap.TBool(False))
print time.ctime()


# In[87]:

# Now, we can keep sorting by different columns, and printing out the output ...

print time.ctime(), "Sorting by Paper PageRank Score ..."
orderby = snap.TStrV()
orderby.Add("PageRankScore")
PaperAA_PRIDJoinT.Order(orderby, "", snap.TBool(False), snap.TBool(False))
print time.ctime()


# In[106]:




# In[95]:

print time.ctime(), "Printing papers_by_prankh ..."
with open('papers_by_prankh.tsv','w') as out_f:
    it = PaperAA_PRIDJoinT.BegRI()
    i = 0
    prev_id = None
    while it < PaperAA_PRIDJoinT.EndRI():
        curr_id = it.GetStrMapByName("PaperID-1")
        auth_seq_num = it.GetIntAttr("AuthSeqNum")
        if prev_id == curr_id:
            if auth_seq_num == 1:
                first_auth = it.GetStrMapByName("AuthorName")
                first_affl = it.GetStrMapByName("AfflName-2")
            elif auth_seq_num == 2:
                first_auth = it.GetStrMapByName("AuthorName")
                first_affl = it.GetStrMapByName("AfflName-2")
            it.Next()
            continue
        if prev_id is not None:
            temp = []
            for obj in [first_auth, first_affl, second_auth, second_affl, paper_title, paper_year]:
                if obj:
                    temp.append(context.GetStr(obj))
                else:
                    temp.append('')
            
            (first_auth, first_affl, second_auth, second_affl, paper_title, paper_year) = tuple(temp)
            print '\t'.join([str(a) for a in [paper_title, paper_year, pr_score, indeg, first_auth, first_affl, second_auth, second_affl]])
        
        first_auth = ''
        first_affl = ''
        second_auth = ''
        second_affl = ''
        paper_title = it.GetStrMapByName("PaperTitle")
        paper_year = it.GetStrMapByName("PaperYear")
        pr_score = it.GetFltAttr("PageRankScore")
        indeg = it.GetIntAttr("InDeg")
        if auth_seq_num == 1:
            first_auth = it.GetStrMapByName("AuthorName")
            first_affl = it.GetStrMapByName("AfflName-2")
        elif auth_seq_num == 2:
            first_auth = it.GetStrMapByName("AuthorName")
            first_affl = it.GetStrMapByName("AfflName-2")
        
        prev_id = curr_id
        it.Next()
        i += 1
        if i > 5 and DEBUG:
            break
print time.ctime()


# In[103]:

print time.ctime(), "Sorting by Paper InDeg ..."
orderby = snap.TStrV()
orderby.Add("InDeg")
PaperAA_PRIDJoinT.Order(orderby, "", snap.TBool(False), snap.TBool(False))
print time.ctime()


# In[ ]:

print time.ctime(), "Printing papers_by_indegv ..."
with open('papers_by_indegv.tsv','w') as out_f:
    it = PaperAA_PRIDJoinT.BegRI()
    i = 0
    prev_id = None
    while it < PaperAA_PRIDJoinT.EndRI():
        curr_id = it.GetStrMapByName("PaperID-1")
        auth_seq_num = it.GetIntAttr("AuthSeqNum")
        if prev_id == curr_id:
            if auth_seq_num == 1:
                first_auth = it.GetStrMapByName("AuthorName")
                first_affl = it.GetStrMapByName("AfflName-2")
            elif auth_seq_num == 2:
                first_auth = it.GetStrMapByName("AuthorName")
                first_affl = it.GetStrMapByName("AfflName-2")
            it.Next()
            continue
        if prev_id is not None:
            temp = []
            for obj in [first_auth, first_affl, second_auth, second_affl, paper_title, paper_year]:
                if obj:
                    temp.append(context.GetStr(obj))
                else:
                    temp.append('')
            
            (first_auth, first_affl, second_auth, second_affl, paper_title, paper_year) = tuple(temp)
            print '\t'.join([str(a) for a in [paper_title, paper_year, pr_score, indeg, first_auth, first_affl, second_auth, second_affl]])
        
        first_auth = ''
        first_affl = ''
        second_auth = ''
        second_affl = ''
        paper_title = it.GetStrMapByName("PaperTitle")
        paper_year = it.GetStrMapByName("PaperYear")
        pr_score = it.GetFltAttr("PageRankScore")
        indeg = it.GetIntAttr("InDeg")
        if auth_seq_num == 1:
            first_auth = it.GetStrMapByName("AuthorName")
            first_affl = it.GetStrMapByName("AfflName-2")
        elif auth_seq_num == 2:
            first_auth = it.GetStrMapByName("AuthorName")
            first_affl = it.GetStrMapByName("AfflName-2")
        
        prev_id = curr_id
        it.Next()
        i += 1
        if i > 5 and DEBUG:
            break
print time.ctime()


# In[96]:

print time.ctime(), "Sorting by Author PageRank ..."
orderby = snap.TStrV()
orderby.Add("PRScoreAggAuth")
PaperAA_PRIDJoinT.Order(orderby, "", snap.TBool(False), snap.TBool(False))
print time.ctime()


# In[99]:

print time.ctime(), "Printing authors by prankh ..."
with open('authors_by_prankh.tsv','w') as out_f:
    it = PaperAA_PRIDJoinT.BegRI()
    i = 0
    prev_id = None
    while it < PaperAA_PRIDJoinT.EndRI():
        curr_id = it.GetStrMapByName("AuthorID-1")
        if prev_id == curr_id:
            paper_year = int(context.GetStr(it.GetStrMapByName("PaperYear")))
            curr_min_year = min(curr_min_year, paper_year)
            curr_max_year = max(curr_max_year, paper_year)
            it.Next()
            continue
        
        if prev_id is not None:            
            print '\t'.join([str(a) for a in [auth_name, auth_aggr_score, auth_count, curr_min_year, curr_max_year]])
    
        #If we reach here, that means we've reached a new paper
    
        auth_name = context.GetStr(it.GetStrMapByName("AuthorName"))
        auth_aggr_score = it.GetFltAttr("PRScoreAggAuth")
        paper_year = int(context.GetStr(it.GetStrMapByName("PaperYear")))
        auth_count = it.GetIntAttr("CountAggAuth")
        curr_min_year = paper_year
        curr_max_year = paper_year
        #print [repr(a) for a in [paper_year, affl_name, affl_aggr_score]]
    
        i += 1
        it.Next()
        prev_id = curr_id
        if i > 5 and DEBUG:
            break
print time.ctime()


# In[104]:

print time.ctime(), "Sorting by Author InDeg ..."
orderby = snap.TStrV()
orderby.Add("InDegAggAuth")
PaperAA_PRIDJoinT.Order(orderby, "", snap.TBool(False), snap.TBool(False))
print time.ctime()


# In[ ]:

print time.ctime(), "Printing authors by InDeg ..."
with open('authors_by_indegv.tsv','w') as out_f:
    it = PaperAA_PRIDJoinT.BegRI()
    i = 0
    prev_id = None
    while it < PaperAA_PRIDJoinT.EndRI():
        curr_id = it.GetStrMapByName("AuthorID-1")
        if prev_id == curr_id:
            paper_year = int(context.GetStr(it.GetStrMapByName("PaperYear")))
            curr_min_year = min(curr_min_year, paper_year)
            curr_max_year = max(curr_max_year, paper_year)
            it.Next()
            continue
        
        if prev_id is not None:            
            print '\t'.join([str(a) for a in [auth_name, auth_aggr_score, auth_count, curr_min_year, curr_max_year]])
    
        #If we reach here, that means we've reached a new paper
    
        auth_name = context.GetStr(it.GetStrMapByName("AuthorName"))
        auth_aggr_score = it.GetFltAttr("PRScoreAggAuth")
        paper_year = int(context.GetStr(it.GetStrMapByName("PaperYear")))
        auth_count = it.GetIntAttr("CountAggAuth")
        curr_min_year = paper_year
        curr_max_year = paper_year
        #print [repr(a) for a in [paper_year, affl_name, affl_aggr_score]]
    
        i += 1
        it.Next()
        prev_id = curr_id
        if i > 5 and DEBUG:
            break
print time.ctime()


# In[100]:

print time.ctime(), "Sorting by Affl PageRank ..."
orderby = snap.TStrV()
orderby.Add("PRScoreAggAffl")
PaperAA_PRIDJoinT.Order(orderby, "", snap.TBool(False), snap.TBool(False))
print time.ctime()


# In[102]:

print time.ctime(), "Printing affls by PRank ... "
with open('affls_by_prankh.tsv','w') as out_f:
    it = PaperAA_PRIDJoinT.BegRI()
    i = 0
    prev_id = None
    while it < PaperAA_PRIDJoinT.EndRI():
        curr_id = it.GetStrMapByName("AfflID-2")
        if prev_id == curr_id:
            paper_year = int(context.GetStr(it.GetStrMapByName("PaperYear")))
            curr_min_year = min(curr_min_year, paper_year)
            curr_max_year = max(curr_max_year, paper_year)
            it.Next()
            continue
        
        if prev_id is not None:
            print '\t'.join([str(a) for a in [affl_name, affl_aggr_score, affl_count, curr_min_year, curr_max_year]])
    
        #If we reach here, that means we've reached a new paper
    
        affl_name = context.GetStr(it.GetStrMapByName("AfflName-2"))
        affl_aggr_score = it.GetFltAttr("PRScoreAggAffl")
        paper_year = int(context.GetStr(it.GetStrMapByName("PaperYear")))
        affl_count = it.GetIntAttr("CountAggAffl")
        curr_min_year = paper_year
        curr_max_year = paper_year
        #print [repr(a) for a in [paper_year, affl_name, affl_aggr_score]]
    
        i += 1
        it.Next()
        prev_id = curr_id
        if i > 5 and DEBUG:
            break
print time.ctime()


# In[105]:

print time.ctime(), "Sorting by Affl InDeg ..."
orderby = snap.TStrV()
orderby.Add("InDegAggAffl")
PaperAA_PRIDJoinT.Order(orderby, "", snap.TBool(False), snap.TBool(False))
print time.ctime()


# In[ ]:

print time.ctime(), "Printing affls by InDeg ..."
with open('affls_by_indegv.tsv','w') as out_f:
    it = PaperAA_PRIDJoinT.BegRI()
    i = 0
    prev_id = None
    while it < PaperAA_PRIDJoinT.EndRI():
        curr_id = it.GetStrMapByName("AfflID-2")
        if prev_id == curr_id:
            paper_year = int(context.GetStr(it.GetStrMapByName("PaperYear")))
            curr_min_year = min(curr_min_year, paper_year)
            curr_max_year = max(curr_max_year, paper_year)
            it.Next()
            continue
        
        if prev_id is not None:
            print '\t'.join([str(a) for a in [affl_name, affl_aggr_score, affl_count, curr_min_year, curr_max_year]])
    
        #If we reach here, that means we've reached a new paper
    
        affl_name = context.GetStr(it.GetStrMapByName("AfflName-2"))
        affl_aggr_score = it.GetFltAttr("PRScoreAggAffl")
        paper_year = int(context.GetStr(it.GetStrMapByName("PaperYear")))
        affl_count = it.GetIntAttr("CountAggAffl")
        curr_min_year = paper_year
        curr_max_year = paper_year
        #print [repr(a) for a in [paper_year, affl_name, affl_aggr_score]]
    
        i += 1
        it.Next()
        prev_id = curr_id
        if i > 5 and DEBUG:
            break
print time.ctime()
