const std = @import("std");
const allocator = std.heap.page_allocator;

pub const log_level = .debug;

const Arguments = struct {
    threads: usize = 1,
    source: []const u8,
};

const Edges = struct {
    src: []u32,
    dst: []u32,
};

const Graph = struct {
    const Node = struct {
        in_edges: []u32,
        out_edges: []u32,
        exists: bool,
    };

    const Prefix = struct {
        offsets: []u32,
        ends: []u32,

        fn init(total_nodes: u32) !Prefix {
            const offsets = try allocator.alloc(u32, total_nodes);
            errdefer allocator.free(offsets);

            const ends = try allocator.alloc(u32, total_nodes);
            errdefer allocator.free(ends);

            return Prefix{
                .offsets = offsets,
                .ends = ends,
            };
        }

        fn calculate(self: *Prefix, ids: []const u32) void {
             // Use the ends array to store the degree of each node
            for (self.ends) |*i| i.* = 0;
            for (ids) |id| self.ends[id] += 1;

            // Calculate the offsets and ends
            var accum: u32 = 0;
            for (self.ends) |*end, i| {
                self.offsets[i] = accum;
                accum += end.*;
                end.* = accum;
            }
        }

        fn deinit(self: Prefix) void {
            allocator.free(self.offsets);
            allocator.free(self.ends);
        }
    };

    nodes: []Node,
    edges: Edges,

    fn initFromTsv(path: []const u8) !Graph {
        const edges = try readTsv(path);
        errdefer allocator.free(edges.src);
        errdefer allocator.free(edges.dst);

        var largest_id: u32 = 0;
        for (edges.src) |id| largest_id = std.math.max(largest_id, id);
        for (edges.dst) |id| largest_id = std.math.max(largest_id, id);

        std.log.info("Largest id: {}", .{ largest_id });
        const total_nodes = largest_id + 1;

        const nodes = try allocator.alloc(Node, total_nodes);
        errdefer allocator.free(nodes);

        for (edges.src) |id| nodes[id].exists = true;
        for (edges.dst) |id| nodes[id].exists = true;

        std.log.info("Calculating src count/prefix...", .{});


        // First, sort `edges.dst` by `edges.src`. To that end, first
        // calculate the prefix sum of `edges.src`.
        var prefix = try Prefix.init(total_nodes);
        prefix.calculate(edges.src);

        for (prefix.offsets) |offset, i| {
            const end = prefix.ends[i];
            nodes[i].out_edges = edges.dst[offset .. end];
        }

        std.log.info("Sort src...", .{});

        // Perform the sort
        // sort both 'src' and 'dst', this makes it possible to sort the edges completely in-place
        swapSort(true, prefix, edges.src, edges.dst);

        std.log.info("Calculating dst count/prefix...", .{});

        // Sort `edges.src` by `edges.dst`.
        prefix.calculate(edges.dst);
        for (prefix.offsets) |offset, i| {
            const end = prefix.ends[i];
            nodes[i].in_edges = edges.src[offset .. end];
        }

        std.log.info("Sort dst...", .{});

        // Perform the sort
        // Note that this time, `edges.dst` isn't changed.
        swapSort(false, prefix, edges.dst, edges.src);

        std.log.debug("Graph loaded", .{});

        return Graph{
            .nodes = nodes,
            .edges = edges,
        };
    }

    fn deinit(self: Graph) void {
        allocator.free(self.edges.src);
        allocator.free(self.edges.dst);
        allocator.free(self.nodes);
    }

    fn swapSort(comptime sort_key: bool, prefix: Prefix, keys: []u32, values: []u32) void {
        for (prefix.offsets) |start_offset, current_key| {
            const end = prefix.ends[current_key];
            var offset = start_offset;
            while (offset < end) : (offset += 1) {
                var key = keys[offset];
                var value = values[offset];

                while (key != current_key) {
                    const target_offset = prefix.offsets[key];
                    prefix.offsets[key] += 1;

                    std.mem.swap(u32, &values[target_offset], &value);
                    if (sort_key) {
                        std.mem.swap(u32, &keys[target_offset], &key);
                    } else {
                        key = keys[target_offset];
                    }
                }

                values[offset] = value;
                if (sort_key) {
                    keys[offset] = key;
                }
            }
        }
    }

    fn numNodes(self: Graph) u32 {
        var total: u32 = 0;
        for (self.nodes) |node| {
            if (node.exists) total += 1;
        }

        return total;
    }

    fn numEdges(self: Graph) usize {
        return self.edges.src.len;
    }

    fn degHist(self: Graph, comptime fields: []const []const u8) ![]usize {
        var max_deg: usize = 0;
        for (self.nodes) |node| {
            var deg: usize = 0;
            inline for (fields) |field| {
                deg += @field(node, field).len;
            }

            max_deg = std.math.max(max_deg, deg);
        }

        const hist = try allocator.alloc(usize, max_deg + 1);
        for (hist) |*i| i.* = 0;

        for (self.nodes) |node| {
            if (!node.exists) continue;

            var deg: usize = 0;
            inline for (fields) |field| {
                deg += @field(node, field).len;
            }
            hist[deg] += 1;
        }

        return hist;
    }

    fn inDegHist(self: Graph) ![]usize {
        return self.degHist(&[_][]const u8{"in_edges"});
    }

    fn outDegHist(self: Graph) ![]usize {
        return self.degHist(&[_][]const u8{"out_edges"});
    }

    fn inOutDegHist(self: Graph) ![]usize {
        return self.degHist(&[_][]const u8{"in_edges", "out_edges"});
    }

    fn wcc(self: Graph) !Udfs {
        var udfs = try Udfs.init(@intCast(u32, self.nodes.len));
        errdefer udfs.deinit();

        for (self.nodes) |node, id| {
            if (!node.exists) continue;
            for (node.out_edges) |dst| udfs.unite(@intCast(u32, id), dst);
        }

        return udfs;
    }

    fn scc(self: *const Graph) !Udfs {
        var tarjan = try Tarjan.init(self);
        defer tarjan.deinit();
        tarjan.tarjan();
        return tarjan.udfs;
    }
};

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

    graph: *const Graph,
    udfs: Udfs,
    index: u32,
    stack: std.ArrayListUnmanaged(u32),
    dfs_stack: std.ArrayListUnmanaged(State),
    info: []NodeInfo,

    fn init(graph: *const Graph) !Tarjan {
        const udfs = try Udfs.init(@intCast(u32, graph.nodes.len));
        errdefer udfs.deinit();

        var stack = try std.ArrayListUnmanaged(u32).initCapacity(allocator, graph.nodes.len);
        errdefer stack.deinit(allocator);

        var dfs_stack = try std.ArrayListUnmanaged(State).initCapacity(allocator, graph.nodes.len);
        errdefer dfs_stack.deinit(allocator);

        const info = try allocator.alloc(NodeInfo, graph.nodes.len);
        errdefer allocator.free(info);
        for (info) |*i| i.* = .{.index = undef, .lowlink = undef, .on_stack = false};

        return Tarjan{
            .graph = graph,
            .udfs = udfs,
            .index = 0,
            .stack = stack,
            .dfs_stack = dfs_stack,
            .info = info,
        };
    }

    fn deinit(self: *Tarjan) void {
        allocator.free(self.info);
        self.dfs_stack.deinit(allocator);
        self.stack.deinit(allocator);
    }

    fn tarjan(self: *Tarjan) void {
        for (self.graph.nodes) |node, id| {
            if (node.exists and self.info[id].index == undef) {
                self.strongconnect(@intCast(u32, id));
            }
        }
    }

    fn strongconnect(self: *Tarjan, v_start: u32) void {
        self.dfs_stack.appendAssumeCapacity(.{.node = v_start, .edge_index = 0});

        dfs: while (self.dfs_stack.popOrNull()) |state| {
            const v = state.node;
            var i = state.edge_index;
            const edges = self.graph.nodes[state.node].out_edges;

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
                    self.udfs.unite(w, v);
                    if (w == v) break;
                }
            }
        }
    }
};

const Udfs = struct {
    p: []i32,
    comps: u32,
    fn init(comps: u32) !Udfs {
        const p = try allocator.alloc(i32, comps);
        for (p) |*i| i.* = -1;

        return Udfs{
            .p = p,
            .comps = comps,
        };
    }

    fn deinit(self: Udfs) void {
        allocator.free(self.p);
    }

    fn find(self: *Udfs, node: u32) u32 {
        if (self.p[node] < 0) {
            return node;
        } else {
            const v = self.find(@intCast(u32, self.p[node]));
            self.p[node] = @intCast(i32, v);
            return v;
        }
    }

    fn unite(self: *Udfs, a: u32, b: u32) void {
        var ac = self.find(a);
        var bc = self.find(b);
        if (ac == bc) return;
        if (self.p[ac] > self.p[bc]) std.mem.swap(u32, &ac, &bc);
        self.p[ac] += self.p[bc];
        self.p[bc] = @intCast(i32, ac);
        self.comps -= 1;
    }

    fn size(self: *Udfs, node: u32) u32 {
        return @intCast(u32, -self.p[self.find(node)]);
    }
};

pub fn main() !void {
    const args = parseArgs() catch std.os.linux.exit(-1);
    const graph = try Graph.initFromTsv(args.source);
    defer graph.deinit();

    const n_nodes = graph.numNodes();
    const invalid_comps = graph.nodes.len - n_nodes;
    std.log.info("# nodes: {}", .{ n_nodes });
    std.log.info("# edges: {}", .{ graph.numEdges() });

    const wudfs = try graph.wcc();
    defer wudfs.deinit();
    std.log.info("# wcc: {}", .{ wudfs.comps - invalid_comps });

    const sudfs = try graph.scc();
    defer sudfs.deinit();
    std.log.info("# scc: {}", .{ sudfs.comps - invalid_comps });

    // const hist = try graph.inOutDegHist();
    // defer allocator.free(hist);

    // for (hist) |count, size| {
    //     if (count != 0) {
    //         std.log.debug("deg {}: {} times", .{ size, count });
    //     }
    // }
}

fn parseArgs() !Arguments {
    var threads: usize = 1;
    var source: ?[]const u8 = null;
    var it = std.process.ArgIterator.init();
    const progname = it.nextPosix();

    while (it.nextPosix()) |arg| {
        if (std.mem.eql(u8, arg, "--help")) {
            std.log.info("Usage: {} [--help] [-t <threads>] <source.tsv>", .{ progname });
            return error.Help;
        } else if (std.mem.eql(u8, arg, "-t")) {
            const threads_str = it.nextPosix() orelse {
                std.log.info("Error: Expected argument <threads> after -t", .{});
                return error.ExpectedArgument;
            };
            threads = std.fmt.parseInt(usize, threads_str, 10) catch |err| {
                std.log.info("Error: '{}' is an invalid number of threads", .{ threads_str });
                return err;
            };
        } else if (source == null) {
            source = arg;
        } else {
            std.log.info("Error: Invalid switch '{}'", .{ arg });
            return error.InvalidSwitch;
        }
    }

    return Arguments{
        .threads = threads,
        .source = source orelse {
            std.log.info("Error: Missing argument <source.tsv>", .{});
            return error.MissingPositional;
        },
    };
}

fn readTsv(path: []const u8) !Edges {
    const tsv_file = try std.fs.cwd().openFile(path, .{});
    defer tsv_file.close();
    const tsv_size = (try tsv_file.stat()).size;
    std.log.debug("Mapping {} bytes...", .{ tsv_size });
    const tsv = try std.os.mmap(null, tsv_size, std.os.PROT_READ, std.os.MAP_PRIVATE, tsv_file.handle, 0);
    defer std.os.munmap(tsv);

    std.log.debug("Counting edges...", .{});
    var n_edges: usize = 0;
    for (tsv) |c| {
        if (c == '\n') n_edges += 1;
    }

    std.log.debug("Allocating for {} edges ({} MiB)...", .{ n_edges, n_edges * @sizeOf(u32) * 2 / (1024 * 1024) });
    const src = try allocator.alloc(u32, n_edges);
    errdefer allocator.free(src);

    const dst = try allocator.alloc(u32, n_edges);
    errdefer allocator.free(dst);

    std.log.debug("Reading edges...", .{});
    var line_it = std.mem.split(tsv, "\n");
    var i: usize = 0;
    while (i < n_edges) : (i += 1) {
        const line = line_it.next().?;
        var field_it = std.mem.split(line, "\t");
        src[i] = try std.fmt.parseInt(u32, field_it.next().?, 10);
        dst[i] = try std.fmt.parseInt(u32, field_it.next().?, 10);
    }

    return Edges{
        .src = src,
        .dst = dst,
    };
}
