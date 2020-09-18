const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Node = struct {
    in_edges: []u32,
    out_edges: []u32,
};

const Edges = struct {
    src: []u32,
    dst: []u32,
};

pub const Graph = struct {
    allocator: *Allocator,
    nodes: []Node,
    edges: Edges,

    pub fn initFromTsv(allocator: *Allocator, path: []const u8) !Graph {
        const edges = try readTsv(allocator, path);
        errdefer allocator.free(edges.src);
        errdefer allocator.free(edges.dst);

        var largest_id: u32 = 0;
        for (edges.src) |id| largest_id = std.math.max(largest_id, id);
        for (edges.dst) |id| largest_id = std.math.max(largest_id, id);

        std.log.info("Largest id: {}", .{ largest_id });
        const total_nodes = largest_id + 1;

        const nodes = try allocator.alloc(Node, total_nodes);
        errdefer allocator.free(nodes);

        // First, sort `edges.dst` by `edges.src`. To that end, first
        // calculate the prefix sum of `edges.src`.
        std.log.info("Calculating src count/prefix...", .{});
        var prefix = try Prefix.init(allocator, total_nodes);
        defer prefix.deinit(allocator);

        prefix.calculate(edges.src);

        for (prefix.offsets) |offset, i| {
            const end = prefix.ends[i];
            nodes[i].out_edges = edges.dst[offset .. end];
        }

        // Perform the sort
        // sort both 'src' and 'dst', this makes it possible to sort the edges completely in-place
        std.log.info("Sort src...", .{});
        prefix.swapSort(true, edges.src, edges.dst);

        // Sort `edges.src` by `edges.dst`.
        std.log.info("Calculating dst count/prefix...", .{});
        prefix.calculate(edges.dst);
        for (prefix.offsets) |offset, i| {
            const end = prefix.ends[i];
            nodes[i].in_edges = edges.src[offset .. end];
        }

        // Perform the sort
        // Note that this time, `edges.dst` isn't changed.
        std.log.info("Sort dst...", .{});
        prefix.swapSort(false, edges.dst, edges.src);

        return Graph{
            .allocator = allocator,
            .nodes = nodes,
            .edges = edges,
        };
    }

    pub fn deinit(self: Graph) void {
        self.allocator.free(self.edges.src);
        self.allocator.free(self.edges.dst);
        self.allocator.free(self.nodes);
    }
};

pub const GraphView = struct {
    pub const Mask = std.PackedIntSlice(u1);

    graph: *const Graph,
    mask: Mask,

    pub fn init(graph: *const Graph) !GraphView {
        const mem_req = Mask.bytesRequired(graph.nodes.len);
        const bytes = try graph.allocator.alloc(u8, mem_req);
        for (bytes) |*b| b.* = 0;
        return GraphView{
            .graph = graph,
            .mask = Mask.init(bytes, graph.nodes.len),
        };
    }

    pub fn initFromNonIsolated(graph: *const Graph) !GraphView {
        var self = try GraphView.init(graph);
        for (graph.edges.src) |id| self.mask.set(id, 1);
        for (graph.edges.dst) |id| self.mask.set(id, 1);
        return self;
    }

    pub fn deinit(self: GraphView) void {
        self.graph.allocator.free(self.mask.bytes);
    }

    pub fn contains(self: GraphView, node: u32) bool {
        return self.mask.get(node) == 1;
    }

    pub fn countNodes(self: GraphView) usize {
        // We know the extra bits in the backing array are zero,
        // so simply do a popcount on every byte of `self.mask.bytes` to
        // get the total number of nodes.
        var total: usize = 0;
        for (self.mask.bytes) |b| total += @popCount(u8, b);
        return total;
    }

    pub fn countEdges(self: GraphView) usize {
        var total: usize = 0;
        for (self.graph.nodes) |node_info, src| {
            if (!self.contains(@intCast(u32, src))) continue;
            for (node_info.out_edges) |dst| {
                if (self.contains(@intCast(u32, dst))) total += 1;
            }
        }
        return total;
    }

    pub fn randomOrdering(self: GraphView) ![]u32 {
        const node_ids = try self.graph.allocator.alloc(u32, self.graph.nodes.len);
        var i: usize = 0;
        for (self.graph.nodes) |_, id| {
            if (self.contains(@intCast(u32, id))) {
                node_ids[i] = @intCast(u32, id);
                i += 1;
            }
        }

        var rng = std.rand.DefaultPrng.init(@truncate(u64, @bitCast(u128, std.time.nanoTimestamp())));
        rng.random.shuffle(u32, node_ids[0 .. i]);

        return self.graph.allocator.realloc(node_ids, i);
    }
};

const Prefix = struct {
    offsets: []u32,
    ends: []u32,

    fn init(allocator: *Allocator, total_nodes: u32) !Prefix {
        const offsets = try allocator.alloc(u32, total_nodes);
        errdefer allocator.free(offsets);

        const ends = try allocator.alloc(u32, total_nodes);
        errdefer allocator.free(ends);

        return Prefix{
            .offsets = offsets,
            .ends = ends,
        };
    }

    fn deinit(self: Prefix, allocator: *Allocator) void {
        allocator.free(self.offsets);
        allocator.free(self.ends);
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

    fn swapSort(self: Prefix, comptime sort_key: bool, keys: []u32, values: []u32) void {
        for (self.offsets) |start_offset, current_key| {
            const end = self.ends[current_key];
            var offset = start_offset;
            while (offset < end) : (offset += 1) {
                var key = keys[offset];
                var value = values[offset];

                while (key != current_key) {
                    const target_offset = self.offsets[key];
                    self.offsets[key] += 1;

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
};

fn readTsv(allocator: *Allocator, path: []const u8) !Edges {
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
