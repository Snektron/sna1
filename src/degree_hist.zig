const GraphView = @import("graph.zig").GraphView;
const Histogram = @import("histogram.zig").Histogram;

pub fn inDegreeHist(gv: GraphView) !Histogram {
    var hist = Histogram.init(gv.graph.allocator);
    errdefer hist.deinit();

    for (gv.graph.nodes) |node_info, id| {
        if (!gv.contains(@intCast(u32, id))) continue;
        try hist.add(node_info.in_edges.len, 1);
    }

    return hist;
}

pub fn outDegreeHist(gv: GraphView) !Histogram {
    var hist = Histogram.init(gv.graph.allocator);
    errdefer hist.deinit();

    for (gv.graph.nodes) |node_info, id| {
        if (!gv.contains(@intCast(u32, id))) continue;
        try hist.add(node_info.out_edges.len, 1);
    }

    return hist;
}

pub fn inOutDegreeHist(gv: GraphView) !Histogram {
    var hist = Histogram.init(gv.graph.allocator);
    errdefer hist.deinit();

    for (gv.graph.nodes) |node_info, id| {
        if (!gv.contains(@intCast(u32, id))) continue;
        try hist.add(node_info.in_edges.len + node_info.out_edges, 1);
    }

    return hist;
}
