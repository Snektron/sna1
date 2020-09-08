import sys
import graph_tool as gt
import graph_tool.topology as gtt

path = sys.argv[1]
g = gt.load_graph_from_csv(path, directed=True, csv_options={'delimiter': '\t'})

print(g.num_edges())
print(g.num_vertices())
wcomp, whist = gtt.label_components(g, directed=False)
print(f"# wcc: {len(whist)}")
scomp, shist = gtt.label_components(g, directed=True)
print(f"# scc: {len(shist)}")
