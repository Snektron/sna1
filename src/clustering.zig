const std = @import("std");
const GraphView = @import("graph.zig").GraphView;
const Ufds = @import("ufds.zig").Ufds;

pub fn avgClusteringCoeff(gv: *const GraphView) f32 {

}

pub fn countTotalTriangles(gv: *const GraphView) usize {
    var total: usize = 0;
    for (gv.graph.nodes) |_, id| {
        if (gv.contains(@intCast(u32, id))) {
            total += countNodeTriangles(gv, @intCast(u32, id));
        }
    }
    return @divExact(total, 6);
}

pub fn countNodeTriangles(gv: *const GraphView, node: u32) usize {
    const nodes = gv.graph.nodes;
    var total: usize = 0;

    for (nodes[node].out_edges) |dst1| {
        if (!gv.contains(dst1)) continue;
        for (nodes[dst1].out_edges) |dst2| {
            if (!gv.contains(dst2)) continue;
            for (nodes[dst2].out_edges) |dst3| {
                if (dst3 == node) total += 1;
            }

            for (nodes[dst2].in_edges) |dst3| {
                if (dst3 == node) total += 1;
            }
        }

        for (nodes[dst1].in_edges) |dst2| {
            if (!gv.contains(dst2)) continue;
            for (nodes[dst2].out_edges) |dst3| {
                if (dst3 == node) total += 1;
            }

            for (nodes[dst2].in_edges) |dst3| {
                if (dst3 == node) total += 1;
            }
        }
    }

    for (nodes[node].in_edges) |dst1| {
        if (!gv.contains(dst1)) continue;
        for (nodes[dst1].out_edges) |dst2| {
            if (!gv.contains(dst2)) continue;
            for (nodes[dst2].out_edges) |dst3| {
                if (dst3 == node) total += 1;
            }

            for (nodes[dst2].in_edges) |dst3| {
                if (dst3 == node) total += 1;
            }
        }

        for (nodes[dst1].in_edges) |dst2| {
            if (!gv.contains(dst2)) continue;
            for (nodes[dst2].out_edges) |dst3| {
                if (dst3 == node) total += 1;
            }

            for (nodes[dst2].in_edges) |dst3| {
                if (dst3 == node) total += 1;
            }
        }
    }

    return total;
}
