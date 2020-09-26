const std = @import("std");
const GraphView = @import("graph.zig").GraphView;
const Histogram = @import("histogram.zig").Histogram;

pub fn completeDistHist(gv: *const GraphView) !Histogram {
    var dist_hist = try DistHist.init(gv);
    errdefer dist_hist.deinit();

    for (gv.graph.nodes) |_, id| {
        if (gv.contains(@intCast(u32, id))) {
            try recordDistances(&dist_hist, @intCast(u32, id));
        }
    }

    dist_hist.deinitExceptHist();
    return dist_hist.hist;
}

pub fn approxDistHist(gv: *const GraphView, samples: usize) !Histogram {
    var dist_hist = try DistHist.init(gv);
    errdefer dist_hist.deinit();

    const nodes = try gv.randomOrdering();
    defer gv.graph.allocator.free(nodes);

    var i: usize = 0;
    for (nodes) |id| {
        try recordDistances(&dist_hist, @intCast(u32, id));
        i += 1;
        if (i == samples) {
            break;
        }
    }

    dist_hist.deinitExceptHist();
    return dist_hist.hist;
}

fn recordDistances(dist_hist: *DistHist, start: u32) !void {
    dist_hist.resetSeen();
    dist_hist.seen.set(start, 1);
    dist_hist.queue.writeItemAssumeCapacity(.{.node = start, .distance = 0});

    while (dist_hist.queue.readItem()) |state| {
        const distance = state.distance + 1;
        for (dist_hist.gv.graph.nodes[state.node].out_edges) |dst| {
            if (dist_hist.gv.contains(dst) and dist_hist.seen.get(dst) == 0) {
                dist_hist.seen.set(dst, 1);
                dist_hist.queue.writeItemAssumeCapacity(.{.node = dst, .distance = distance});
                try dist_hist.hist.add(distance, 1);
            }
        }
    }
}

const State = struct {
    node: u32,
    distance: u32,
};

const DistHist = struct {
    const Queue = std.fifo.LinearFifo(State, .Slice);

    gv: *const GraphView,
    queue_mem: []State,
    queue: Queue,
    seen: GraphView.Mask,
    hist: Histogram,

    fn init(gv: *const GraphView) !DistHist {
        const hist = Histogram.init(gv.graph.allocator);
        errdefer hist.deinit();

        const seen_bytes = try gv.graph.allocator.alloc(u8, gv.mask.bytes.len);
        errdefer gv.graph.allocator.free(seen_bytes);
        // `seen_bytes` is cleared in when `resetSeen` is called first.

        const queue_mem = try gv.graph.allocator.alloc(State, gv.graph.nodes.len);

        return DistHist{
            .gv = gv,
            .queue_mem = queue_mem,
            .queue = Queue.init(queue_mem),
            .seen = GraphView.Mask.init(seen_bytes, gv.graph.nodes.len),
            .hist = hist,
        };
    }

    fn resetSeen(self: *DistHist) void {
        for (self.seen.bytes) |*b| b.* = 0;
    }

    fn deinit(self: DistHist) void {
        self.deinitExceptHist();
        self.hist.deinit();
    }

    fn deinitExceptHist(self: DistHist) void {
        self.gv.graph.allocator.free(self.queue_mem);
        self.gv.graph.allocator.free(self.seen.bytes);
    }
};
