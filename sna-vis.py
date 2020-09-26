import sys
import graph_tool as gt
import graph_tool.topology as gtt
import graph_tool.draw as gtd
import graph_tool.generation as gtg
import graph_tool.inference as gti
import math

path = sys.argv[1]
g = gt.load_graph_from_csv(path, directed=True, csv_options={'delimiter': '\t'})

# g = gtg.price_network(300)
print("generating layout")
# pos = gtd.radial_tree_layout(g, g.vertex(0))
state = gti.minimize_nested_blockmodel_dl(g, deg_corr=True)
print("drawing")
gtd.draw_hierarchy(state, layout="sfdp", output="medium.pdf")
# gtd.graph_draw(g, pos=pos, output="medium.pdf")
