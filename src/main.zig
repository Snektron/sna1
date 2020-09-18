const std = @import("std");
const g = @import("graph.zig");
const degree_hist = @import("degree_hist.zig");
const components = @import("components.zig");
const Ufds = @import("ufds.zig").Ufds;
const dist_hist = @import("dist_hist.zig");
const clustering = @import("clustering.zig");

pub const log_level = .debug;

pub fn processUfds(name: []const u8, ufds: *Ufds) !void {
    std.log.info("{}:", .{ name });
    std.log.info(" - # components: {}", .{ ufds.num_comps });

    const largest = ufds.findLargestComponent();
    const gv = try ufds.extract(largest);
    defer gv.deinit();

    std.log.info(" - # nodes: {}", .{ gv.countNodes() });
    std.log.info(" - # edges: {}", .{ gv.countEdges() });
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

    var cc = try clustering.avgClusteringCoeff(&view);
    std.log.info(" - # avg clustering coefficient: {d}", .{ cc });

    var i: usize = 100;
    while (i < 2000) : (i += 100) {
        var acc = try clustering.approxAvgClusteringCoeff(&view, i);
        std.log.info(" - {}: # approx avg clustering coefficient: {d}", .{ i, acc });
    }

    const dh = try dist_hist.approxDistHist(&view, 10);
    defer dh.deinit();
    dh.dump();

    const dh2 = try dist_hist.completeDistHist(&view);
    defer dh2.deinit();
    dh2.dump();

    // {
    //     var wufds = try components.wcc(&view);
    //     defer wufds.deinit();
    //     try processUfds("wcc", &wufds);
    // }

    // {
    //     var sufds = try components.scc(&view);
    //     defer sufds.deinit();
    //     try processUfds("scc", &sufds);
    // }
}
