const std = @import("std");
const g = @import("graph.zig");
const degree_hist = @import("degree_hist.zig");
const cc = @import("connected_components.zig");
const Ufds = @import("ufds.zig").Ufds;
const dist_hist = @import("dist_hist.zig");

pub const log_level = .debug;

pub fn dumpUfdsInfo(name: []const u8, ufds: *Ufds) !void {
    std.log.info("{}:", .{ name });
    std.log.info(" - # components: {}", .{ ufds.num_comps });

    const largest = ufds.findLargestComponent();
    const gv = try ufds.extract(largest);
    defer gv.deinit();

    std.log.info(" - # nodes: {}", .{ gv.countNodes() });
    std.log.info(" - # edges: {}", .{ gv.countEdges() });

    const hist = try dist_hist.approxDistHist(&gv, 100);
    defer hist.deinit();
    hist.dump();
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    var args = std.process.ArgIterator.init();
    const progname = args.nextPosix().?;
    const tsv_path = args.nextPosix() orelse {
        std.log.info("Usage: {} <graph.tsv>", .{progname});
        std.os.linux.exit(-1);
    };

    const graph = try g.Graph.initFromTsv(allocator, tsv_path);
    defer graph.deinit();
    std.log.info("Graph loaded", .{});

    const view = try g.GraphView.initFromNonIsolated(&graph);
    defer view.deinit();

    std.log.info("# nodes: {}", .{ view.countNodes() });
    std.log.info("# edges: {}", .{ graph.edges.src.len });

    {
        var wufds = try cc.wcc(&view);
        defer wufds.deinit();
        try dumpUfdsInfo("wcc", &wufds);
    }

    {
        var sufds = try cc.scc(&view);
        defer sufds.deinit();
        try dumpUfdsInfo("scc", &sufds);
    }
}
