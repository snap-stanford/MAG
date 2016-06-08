
FIn = snap.TFIn("mmnet.bin")
mmnet = snap.TMMNet.Load(FIn)

crossnetnames = snap.TStrV()
crossnetnames.Add("refs")
print time.ctime()
refs_pneanet = mmnet.ToNetworkMP(crossnetnames)
print time.ctime()

snap.PlotInDegDistr(refs_pneanet, "refs_indeg", "References graph - in-degree Distribution")

print time.ctime()
snap.PlotOutDegDistr(refs_pneanet, "refs_outdeg", "References graph - out-degree distribution")
print time.ctime()
