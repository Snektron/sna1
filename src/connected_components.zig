const std = @import("std");
const GraphView = @import("graph.zig").GraphView;
const Ufds = @import("ufds.zig").Ufds;

pub fn wcc(gv: *const GraphView) !Ufds {
    var ufds = try Ufds.init(gv);
    errdefer ufds.deinit();

    for (gv.graph.nodes) |node_info, src| {
        if (!gv.contains(@intCast(u32, src))) continue;
        for (node_info.out_edges) |dst| {
            if (gv.contains(dst)) {
                ufds.unite(@intCast(u32, src), dst);
            }
        }
    }

    return ufds;
}

pub fn scc(gv: *const GraphView) !Ufds {
    var tarjan = try Tarjan.init(gv);
    defer tarjan.deinit();
    return tarjan.tarjan();
}

const Tarjan = struct {
    const undef = 0xFFFF_FFFF;

    const NodeInfo = struct {
        index: u32,
        lowlink: u32,
        on_stack: bool,
    };

    const State = struct {
        node: u32,
        edge_index: u32,
    };

    gv: *const GraphView,
    index: u32,
    stack: std.ArrayList(u32),
    dfs_stack: std.ArrayList(State),
    info: []NodeInfo,

    fn init(gv: *const GraphView) !Tarjan {
        const largest_id = gv.graph.nodes.len;
        const allocator = gv.graph.allocator;

        var stack = try std.ArrayList(u32).initCapacity(allocator, largest_id);
        errdefer stack.deinit();

        var dfs_stack = try std.ArrayList(State).initCapacity(allocator, largest_id);
        errdefer dfs_stack.deinit();

        const info = try allocator.alloc(NodeInfo, largest_id);
        errdefer allocator.free(info);
        for (info) |*i| i.* = .{.index = undef, .lowlink = undef, .on_stack = false};

        return Tarjan{
            .gv = gv,
            .index = 0,
            .stack = stack,
            .dfs_stack = dfs_stack,
            .info = info,
        };
    }

    fn deinit(self: *Tarjan) void {
        self.gv.graph.allocator.free(self.info);
        self.dfs_stack.deinit();
        self.stack.deinit();
    }

    fn tarjan(self: *Tarjan) !Ufds {
        var ufds = try Ufds.init(self.gv);

        for (self.gv.graph.nodes) |_, id| {
            if (self.gv.contains(@intCast(u32, id)) and self.info[id].index == undef) {
                self.strongconnect(&ufds, @intCast(u32, id));
            }
        }

        return ufds;
    }

    fn strongconnect(self: *Tarjan, ufds: *Ufds, v_start: u32) void {
        self.dfs_stack.appendAssumeCapacity(.{.node = v_start, .edge_index = 0});

        dfs: while (self.dfs_stack.popOrNull()) |state| {
            const v = state.node;
            var i = state.edge_index;
            const edges = self.gv.graph.nodes[state.node].out_edges;

            if (i == 0) {
                self.info[v] = .{.index = self.index, .lowlink = self.index, .on_stack = true};
                self.index += 1;
                self.stack.appendAssumeCapacity(v);
            } else {
                const w = edges[i - 1];
                self.info[v].lowlink = std.math.min(self.info[v].lowlink, self.info[w].lowlink);
            }

            while (i < edges.len) : (i += 1) {
                const w = edges[i];
                if (self.info[w].index == undef) {
                    // Recurse
                    self.dfs_stack.appendAssumeCapacity(.{.node = v, .edge_index = i + 1});
                    self.dfs_stack.appendAssumeCapacity(.{.node = w, .edge_index = 0});
                    continue :dfs;
                } else if (self.info[w].on_stack) {
                    self.info[v].lowlink = std.math.min(self.info[v].lowlink, self.info[w].index);
                }
            }

            if (self.info[v].lowlink == self.info[v].index) {
                while (true) {
                    const w = self.stack.pop();
                    self.info[w].on_stack = false;
                    ufds.unite(w, v);
                    if (w == v) break;
                }
            }
        }
    }
};
