const std = @import("std");
const GraphView = @import("graph.zig").GraphView;
const Ufds = @import("ufds.zig").Ufds;

pub fn avgClusteringCoeff(gv: *const GraphView) f32 {
    var total: f32 = 0;
    var n: usize = 0;
    for (gv.graph.nodes) |node_info, id| {
        if (!gv.contains(@intCast(u32, id))) continue;
        total += clusteringCoeff(gv, @intCast(u32, id)) orelse continue;
        n += 1;
    }
    return total / @intToFloat(f32, n);
}

pub fn clusteringCoeff(gv: *const GraphView, node: u32) ?f32 {
    const tris = countNodeTris(gv, node);
    if (tris == 0) return null;
    const triangles = countNodeTriangles(gv, node);
    return @intToFloat(f32, triangles) / @intToFloat(f32, tris);
}

pub fn countNodeTris(gv: *const GraphView, node: u32) usize {
    var k: usize = 0;
    for (gv.graph.nodes[node].in_edges) |dst| {
        if (gv.contains(dst)) k += 1;
    }
    for (gv.graph.nodes[node].out_edges) |dst| {
        if (gv.contains(dst)) k += 1;
    }
    if (k == 0) return 0;
    return k * (k - 1);
}

pub fn countTotalTriangles(gv: *const GraphView) usize {
    var total: usize = 0;
    for (gv.graph.nodes) |_, id| {
        if (gv.contains(@intCast(u32, id))) {
            total += countNodeTriangles(gv, @intCast(u32, id));
        }
    }
    return @divExact(total, 3);
}

fn countNodeTriangles(gv: *const GraphView, node: u32) usize {
    const nodes = gv.graph.nodes;
    var total: usize = 0;

    var hm = std.AutoArrayHashMap(u32, void).init(gv.graph.allocator);
    defer hm.deinit();

    var hm1 = std.AutoArrayHashMap(u32, void).init(gv.graph.allocator);
    defer hm1.deinit();

    for (nodes[node].out_edges) |dst| {
        if (gv.contains(@intCast(u32, dst))) hm.put(dst, {}) catch unreachable;
    }
    for (nodes[node].in_edges) |dst| {
        if (gv.contains(@intCast(u32, dst))) hm.put(dst, {}) catch unreachable;
    }

    for (hm.items()) |entry| {
        const dst1 = entry.key;

        hm1.clearRetainingCapacity();

        for (nodes[dst1].out_edges) |dst2| hm1.put(dst2, {}) catch unreachable;
        for (nodes[dst1].in_edges) |dst2| hm1.put(dst2, {}) catch unreachable;

        for (hm1.items()) |entry1| {
            if (hm.contains(entry1.key)) total += 1;
        }
    }

    return @divExact(total, 2);
}
