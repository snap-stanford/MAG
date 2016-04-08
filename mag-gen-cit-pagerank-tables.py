
# coding: utf-8

# In[1]:



# In[1]:

# %load mag-refs-load.py
# In[1]:

import sys
import os
import time
import snap
print snap.Version
import testutils

context = snap.TTableContext()

t = testutils.Timer()
r = testutils.Resource()

# load tables and context
srcfile = "mag-022016-papers-refs-context-net.bin"
FIn = snap.TFIn(srcfile)

# load papers
print time.ctime(), "loading papers ..."
papers = snap.TTable.Load(FIn,context)
t.show("loadbin papers", papers)
r.show("__papers__")

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


# In[3]:

print time.ctime(), "Loading indegv into python dict ..."
indegv_dict = {}
n = 0
for item in InDegV:
    indegv_dict[item.GetVal1()] = item.GetVal2()
    n += 1
    if n % 10000000 == 0:
        print n


# In[4]:

#print len(indegv_dict)
print time.ctime(), "Sorting indegv key dict ..."
sorted_indegv_keys = sorted(indegv_dict, key=indegv_dict.get)


# In[5]:

#print sorted_indegv_keys[-1000:]


# In[6]:

print time.ctime(), "computing pagerank ..."
PRankH = snap.TIntFltH()
snap.GetPageRankMP(net, PRankH, 0.85, 1e-4, 100)
t.show("prank", PRankH)
r.show("__PRankH__")


# In[7]:

print time.ctime(), "Loading prankh into python dict ..."
prankh_dict = {}
n = 0
for item in PRankH:
    prankh_dict[item] = PRankH[item]
    n += 1
    if n % 10000000 == 0:
        print n


# In[8]:

#print len(prankh_dict)
print time.ctime(), "sorting prankh dict ..."
sorted_prankh_keys = sorted(prankh_dict, key=prankh_dict.get)


# In[9]:

#print sorted_prankh_keys[-1000:]


# In[10]:
"""
print time.ctime(), "computing K-score sizes ..."
CoreIDSzV = snap.TIntPrV()
kValue = snap.GetKCoreNodes(net, CoreIDSzV)
print kValue
t.show("kcore", kValue)
r.show("__kValue__")


# In[11]:

k = 634
print time.ctime(), "computing largest K-core ..."
KCore = snap.GetKCore(net, k)
t.show("kcore", KCore)
r.show("__kcore__")


# In[12]:

print KCore.GetNodes()
kcorenidv = snap.TIntV()
KCore.GetNIdV(kcorenidv)


# In[13]:

print kcorenidv.Len()
kcorenid_l = []
for nid in kcorenidv:
    kcorenid_l.append(nid)

"""
# In[14]:

#indegvidlist = []
#for key in sorted_indegv_keys:
#    indegvidlist.append(context.GetStr(key))


# In[15]:

#print indegvidlist[:10]


# In[16]:

#pranklist = []
#for key in sorted_prankh_keys:
#    pranklist.append(context.GetStr(key))
#print pranklist[:10]


# In[ ]:

# In[17]:
"""
kcorelist = []
for key in kcorenid_l:
    kcorelist.append(context.GetStr(key))
print kcorelist
"""

# In[ ]:
print time.ctime(), "Hashing papers ..."
papers_f = open('/dfs/scratch0/viswa/mag022016/Papers.txt', 'r')
i = 0
pap_to_title_dict = {}
for line in papers_f:
    spline = line.split('\t')
    pap_to_title_dict[spline[0]] = spline[1:]
    i += 1
    if i % 1000000 == 0:
        print i


# In[ ]:
"""
# indegvidlist, pranklist, kcorelist
for pid in reversed(indegvidlist):
    print pap_to_title_dict[pid]


# In[ ]:

for pid in reversed(pranklist):
    print pap_to_title_dict[pid]


# In[ ]:

for pid in kcorelist:
    print pap_to_title_dict[pid]
"""

# In[ ]:
print time.ctime(), "Hashing PaperAA"
paperaa_f = open('/dfs/scratch0/viswa/mag022016/PaperAuthorAffiliations.txt', 'r')
i = 0
pap_to_aa_dict = {}
for line in paperaa_f:
    spline = line.split('\t')
    if spline[0] in pap_to_aa_dict:
        pap_to_aa_dict[spline[0]].append(spline[1:])
    else:
        pap_to_aa_dict[spline[0]] = [spline[1:]]
    i += 1
    if i % 1000000 == 0:
        print i


# In[ ]:
print time.ctime(), "Hashing Authors to names"
authors_f = open('/dfs/scratch0/viswa/mag022016/Authors.txt', 'r')
i = 0
auth_to_name_dict = {}
for line in authors_f:
    spline = line.split('\t')
    auth_to_name_dict[spline[0]] = spline[1]
    i += 1
    if i % 1000000 == 0:
        print i

print time.ctime(), "Hashing Affls to names"
affl_f = open('/dfs/scratch0/viswa/mag022016/Affiliations.txt', 'r')
i = 0
affl_to_name_dict = {}
for line in affl_f:
    spline = line.split('\t')
    affl_to_name_dict[spline[0]] = spline[1]
    i += 1
    if i % 1000000 == 0:
        print i


# In[ ]:


# In[ ]:

i = 0
print time.ctime(), "Writing papers_by_indegv file ..."
indegv_f = open('papers_by_indegv.tsv','w')
for paper in reversed(sorted_indegv_keys):
    pid = context.GetStr(paper)
    cit_cnt = indegv_dict[paper]
    pinfo = pap_to_title_dict[pid]
    title = pinfo[0]
    year = pinfo[2]
    venue = pinfo[5]
    aainfos = pap_to_aa_dict[pid]
    first = ''
    first_affl = ''
    second = ''
    second_affl = ''
    for aainfo in aainfos:
        pos = int(aainfo[-1])
        auth = str(aainfo[0])
        affl = str(aainfo[1])
        if pos == 1:
            if len(auth) > 0:
                first = auth_to_name_dict[auth][:-2]
            if len(affl) > 0:
                first_affl = affl_to_name_dict[affl][:-2]
        elif pos == 2:
            if len(auth) > 0:
                second = auth_to_name_dict[auth][:-2]
            if len(affl) > 0:
                second_affl = affl_to_name_dict[affl][:-2]
    indegv_f.write('\t'.join([str(a) for a in [pid, title, cit_cnt, year, venue, first, first_affl, second, second_affl]]))
    indegv_f.write('\n')
    i += 1
    if i % 1000000 == 0:
        print i
indegv_f.close()
print time.ctime()


# In[ ]:

i = 0
print time.ctime(), "Writing papers_by_prankh file ..."
prankh_f = open('papers_by_prankh.tsv','w')
for paper in reversed(sorted_prankh_keys):
    pid = context.GetStr(paper)
    score = prankh_dict[paper]
    pinfo = pap_to_title_dict[pid]
    title = pinfo[0]
    year = pinfo[2]
    venue = pinfo[5]
    aainfos = pap_to_aa_dict[pid]
    first = ''
    first_affl = ''
    second = ''
    second_affl = ''
    for aainfo in aainfos:
        pos = int(aainfo[-1])
        auth = str(aainfo[0])
        affl = str(aainfo[1])
        if pos == 1:
            if len(auth) > 0:
                first = auth_to_name_dict[auth][:-2]
            if len(affl) > 0:
                first_affl = affl_to_name_dict[affl][:-2]
        elif pos == 2:
            if len(auth) > 0:
                second = auth_to_name_dict[auth][:-2]
            if len(affl) > 0:
                second_affl = affl_to_name_dict[affl][:-2]
    prankh_f.write('\t'.join([str(a) for a in [pid, title, score, year, venue, first, first_affl, second, second_affl]]))
    prankh_f.write('\n')
    i += 1
    if i % 1000000 == 0:
        print i
prankh_f.close()
print time.ctime()


# In[ ]:

authors_to_pranks = {}
affl_to_pranks = {}
auth_to_startyear = {}
auth_to_endyear = {}
auth_to_count = {}
auth_to_cit_count = {}
affl_to_startyear = {}
affl_to_endyear = {}
affl_to_count = {}
affl_to_cit_count = {}
i = 0
print time.ctime(), "Computing scores of authors and affiliations ..."
for paper in prankh_dict:
    i += 1
    if i % 1000000 == 0:
        print i
    score = prankh_dict[paper]
    cit_cnt = indegv_dict[paper]
    pid = context.GetStr(paper)
    year = int(pap_to_title_dict[pid][2])
    aainfos = pap_to_aa_dict[pid]
    for aainfo in aainfos:
        (auth_id, affl_id) = aainfo[:2]
        if auth_id not in authors_to_pranks:
            authors_to_pranks[auth_id] = score
            auth_to_startyear[auth_id] = year
            auth_to_endyear[auth_id] = year
            auth_to_count[auth_id] = 1
            auth_to_cit_count[auth_id] = cit_cnt
        else:
            authors_to_pranks[auth_id] = authors_to_pranks[auth_id] + score
            auth_to_startyear[auth_id] = min(year, auth_to_startyear[auth_id])
            auth_to_endyear[auth_id] = max(year, auth_to_endyear[auth_id])
            auth_to_count[auth_id] = auth_to_count[auth_id] + 1
            auth_to_cit_count[auth_id] = auth_to_cit_count[auth_id] + cit_cnt
        if len(affl_id) == 0:
            continue
        if affl_id not in affl_to_pranks:
            affl_to_pranks[affl_id] = score
            affl_to_startyear[affl_id] = year
            affl_to_endyear[affl_id] = year
            affl_to_count[affl_id] = 1
            affl_to_cit_count[affl_id] = cit_cnt
        else:
            affl_to_pranks[affl_id] = affl_to_pranks[affl_id] + score
            affl_to_startyear[affl_id] = min(year, affl_to_startyear[affl_id])
            affl_to_endyear[affl_id] = max(year, affl_to_endyear[affl_id])
            affl_to_count[affl_id] = affl_to_count[affl_id] + 1
            affl_to_cit_count[affl_id] = affl_to_cit_count[affl_id] + cit_cnt
print time.ctime()


# In[ ]:

# In[ ]:
print time.ctime(), "Sorting authors by PageRank ..."
sorted_auths = sorted(authors_to_pranks, key=authors_to_pranks.get)
print time.ctime(), "Sorting affls by PageRank ..."
sorted_affls = sorted(affl_to_pranks, key=affl_to_pranks.get)
print time.ctime()

# In[ ]:


# In[ ]:

i = 0
print time.ctime(), "Writing authors_by_prankh ..."
auth_f = open('authors_by_prankh.tsv', 'w')
for auth in reversed(sorted_auths):
    name = auth_to_name_dict[auth][:-2]
    count = auth_to_count[auth]
    cit_cnt = auth_to_cit_count[auth]
    startyear = auth_to_startyear[auth]
    endyear = auth_to_endyear[auth]
    score = authors_to_pranks[auth]
    auth_f.write('\t'.join([str(s) for s in [auth,name,score,count,cit_cnt,startyear,endyear]]))
    auth_f.write('\n')
    i += 1
    if i%1000000 == 0:
        print i
auth_f.close()
print time.ctime()


# In[ ]:

affl_f = open('affls_by_prankh.tsv', 'w')
i = 0
print time.ctime(), "Writing affls_by_prankh ..."
for affl in reversed(sorted_affls):
    if len(affl) == 0:
        continue
    name = affl_to_name_dict[affl][:-2]
    count = affl_to_count[affl]
    score = affl_to_pranks[affl]
    cit_cnt = affl_to_cit_count[affl]
    startyear = affl_to_startyear[affl]
    endyear = affl_to_endyear[affl]
    affl_f.write('\t'.join([str(s) for s in [affl,name,score,count,cit_cnt,startyear,endyear]]))
    affl_f.write('\n')
    i += 1
    if i%100000 == 0:
        print i
affl_f.close()
print time.ctime()


# In[ ]:

print time.ctime(), "Sorting authors by citation count ..."
sorted_auths_by_cit = sorted(auth_to_cit_count, key=auth_to_cit_count.get)
print time.ctime(), "Sorting affls by citation count ..."
sorted_affls_by_cit = sorted(affl_to_cit_count, key=affl_to_cit_count.get)
print time.ctime(), "Writing authors_by_cit ... "
i = 0
auth_f = open('authors_by_cit.tsv', 'w')
for auth in reversed(sorted_auths_by_cit):
    name = auth_to_name_dict[auth][:-2]
    count = auth_to_count[auth]
    cit_cnt = auth_to_cit_count[auth]
    startyear = auth_to_startyear[auth]
    endyear = auth_to_endyear[auth]
    score = authors_to_pranks[auth]
    auth_f.write('\t'.join([str(s) for s in [auth,name,score,count,cit_cnt,startyear,endyear]]))
    auth_f.write('\n')
    i += 1
    if i%1000000 == 0:
        print i
auth_f.close()
print time.ctime(), "Writing affls_by_cit ... "
affl_f = open('affls_by_cit.tsv', 'w')
i = 0
for affl in reversed(sorted_affls_by_cit):
    if len(affl) == 0:
        continue
    name = affl_to_name_dict[affl][:-2]
    count = affl_to_count[affl]
    score = affl_to_pranks[affl]
    cit_cnt = affl_to_cit_count[affl]
    startyear = affl_to_startyear[affl]
    endyear = affl_to_endyear[affl]
    affl_f.write('\t'.join([str(s) for s in [affl,name,score,count,cit_cnt,startyear,endyear]]))
    affl_f.write('\n')
    i += 1
    if i%100000 == 0:
        print i
affl_f.close()
print time.ctime()
