const std = @import("std");
const GraphView = @import("graph.zig").GraphView;
const Ufds = @import("ufds.zig").Ufds;

pub fn avgClusteringCoeff(gv: *const GraphView) !f32 {
    var total: f32 = 0;
    var counter = TriangleCounter.init(gv);
    var n: usize = 0;
    for (gv.graph.nodes) |node_info, id| {
        if (!gv.contains(@intCast(u32, id))) continue;
        total += (try counter.clusteringCoeff(@intCast(u32, id))) orelse continue;
        n += 1;
    }
    return total / @intToFloat(f32, n);
}

pub fn approxAvgClusteringCoeff(gv: *const GraphView, samples: usize) !f32 {
    const nodes = try gv.randomSubset(samples);
    defer gv.graph.allocator.free(nodes);
    var counter = TriangleCounter.init(gv);

    var total: f32 = 0;
    var i: usize = 0;

    for (nodes) |id| {
        total += (try counter.clusteringCoeff(@intCast(u32, id))) orelse continue;
        i += 1;
    }

    return total / @intToFloat(f32, i);
}

const TriangleCounter = struct {
    const NodeSet = std.AutoArrayHashMap(u32, void);

    gv: *const GraphView,
    dst1_edges: NodeSet,
    dst2_edges: NodeSet,

    fn init(gv: *const GraphView) TriangleCounter {
        return TriangleCounter{
            .gv = gv,
            .dst1_edges = NodeSet.init(gv.graph.allocator),
            .dst2_edges = NodeSet.init(gv.graph.allocator),
        };
    }

    fn deinit(self: TriangleCounter) void {
        self.dst2_edges.deinit();
        self.dst1_edges.deinit();
    }

    fn clusteringCoeff(self: *TriangleCounter, node: u32) !?f32 {
        var triangles: usize = 0;
        const nodes = self.gv.graph.nodes;
        self.dst1_edges.clearRetainingCapacity();

        for (nodes[node].out_edges) |dst| {
            if (self.gv.contains(@intCast(u32, dst))) try self.dst1_edges.put(dst, {});
        }
        for (nodes[node].in_edges) |dst| {
            if (self.gv.contains(@intCast(u32, dst))) try self.dst1_edges.put(dst, {});
        }

        const k = self.dst1_edges.items().len;
        if (k == 0 or k == 1) return null;
        const tris = k * (k - 1) / 2;

        for (self.dst1_edges.items()) |entry| {
            const dst1 = entry.key;

            self.dst2_edges.clearRetainingCapacity();

            for (nodes[dst1].out_edges) |dst2| try self.dst2_edges.put(dst2, {});
            for (nodes[dst1].in_edges) |dst2| try self.dst2_edges.put(dst2, {});

            for (self.dst2_edges.items()) |entry1| {
                if (self.dst1_edges.contains(entry1.key)) triangles += 1;
            }
        }

        return @intToFloat(f32, @divExact(triangles, 2)) / @intToFloat(f32, tris);
    }
};
