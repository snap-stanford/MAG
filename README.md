# MAG
Programs for Microsoft Academic Graph

This repo consists of programs to analyze the Microsoft Academic Graph.

mag-refs-save.py inputs the data and converts it into TTable, saving it as a SNAP binary.

mag-gen-cit-pagerank-tables.py reads these binaries, creates the network and outputs:

1. Top Papers By PageRank
2. Top Papers By Citations
3. Top Authors By Sum of Paper PageRanks
4. Top Authors by Sum of Paper Citations
3. Top Affiliations By Sum of Paper PageRanks
4. Top Affiliations by Sum of Paper Citations

The sample_out dir contains the top 1000 lines of the outputs of these files.
