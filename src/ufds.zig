const std = @import("std");
const GraphView = @import("graph.zig").GraphView;

pub const Ufds = struct {
    gv: *const GraphView,
    parents: []i32,
    num_comps: usize,

    pub fn init(gv: *const GraphView) !Ufds {
        const parents = try gv.graph.allocator.alloc(i32, gv.graph.nodes.len);
        for (parents) |*p| p.* = -1;

        return Ufds{
            .gv = gv,
            .parents = parents,
            .num_comps = gv.countNodes(),
        };
    }

    pub fn deinit(self: Ufds) void {
        self.gv.graph.allocator.free(self.parents);
    }

    pub fn find(self: *Ufds, node: u32) u32 {
        if (self.parents[node] < 0) {
            return node;
        } else {
            const comp = self.find(@intCast(u32, self.parents[node]));
            self.parents[node] = @intCast(i32, comp);
            return comp;
        }
    }

    pub fn unite(self: *Ufds, a: u32, b: u32) void {
        var ac = self.find(a);
        var bc = self.find(b);
        if (ac == bc) return;
        if (self.parents[ac] > self.parents[bc]) std.mem.swap(u32, &ac, &bc);
        self.parents[ac] += self.parents[bc];
        self.parents[bc] = @intCast(i32, ac);
        self.num_comps -= 1;
    }

    pub fn size(self: *Ufds, node: u32) u32 {
        return @intCast(u32, -self.parents[self.find(node)]);
    }

    pub fn findLargestComponent(self: *Ufds) u32 {
        var largestSize: i32 = 0;
        var largestComp: u32 = 0;
        for (self.parents) |parent, node| {
            if (self.gv.contains(@intCast(u32, node)) and parent < 0 and parent < largestSize) {
                largestSize = parent;
                largestComp = @intCast(u32, node);
            }
        }
        return largestComp;
    }

    pub fn extract(self: *Ufds, node: u32) !GraphView {
        const comp = self.find(node);
        var gv = try GraphView.init(self.gv.graph);

        for (self.gv.graph.nodes) |_, id| {
            if (self.find(@intCast(u32, id)) == comp) {
                gv.mask.set(id, 1);
            }
        }

        return gv;
    }
};
