const std = @import("std");
const g = @import("graph.zig");
const degree_hist = @import("degree_hist.zig");
const components = @import("components.zig");
const Ufds = @import("ufds.zig").Ufds;
const dist_hist = @import("dist_hist.zig");
const clustering = @import("clustering.zig");

pub const log_level = .debug;

pub fn processUfds(name: []const u8, ufds: *Ufds) !g.GraphView {
    std.log.info("{}:", .{ name });
    std.log.info(" - # components: {}", .{ ufds.num_comps });

    const largest = ufds.findLargestComponent();
    const gv = try ufds.extract(largest);
    errdefer gv.deinit();

    std.log.info(" - # nodes: {}", .{ gv.countNodes() });
    std.log.info(" - # edges: {}", .{ gv.countEdges() });

    return gv;
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

    const in = try degree_hist.inDegreeHist(&view);
    defer in.deinit();
    std.log.info("In degree histogram:", .{});
    in.dump();

    const out = try degree_hist.outDegreeHist(&view);
    defer out.deinit();
    std.log.info("Out degree histogram:", .{});
    out.dump();

    {
        var wufds = try components.wcc(&view);
        defer wufds.deinit();

        const wgv = try processUfds("wcc", &wufds);
        defer wgv.deinit();

        const cc = try clustering.avgClusteringCoeff(&wgv);
        // const cc = try clustering.approxAvgClusteringCoeff(&view, 25000);
        std.log.info(" - avg clustering coefficient: {d}", .{ cc });

        const dh = try dist_hist.completeDistHist(&view);
        // const dh = try dist_hist.approxDistHist(&view, 100);
        defer dh.deinit();
        dh.dump();
    }

    {
        var sufds = try components.scc(&view);
        defer sufds.deinit();

        const sgv = try processUfds("scc", &sufds);
        defer sgv.deinit();
    }
}
