const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Histogram = struct {
    values: std.ArrayList(usize),

    pub fn init(allocator: *Allocator) Histogram {
        return Histogram{
            .values = std.ArrayList(usize).init(allocator)
        };
    }

    pub fn deinit(self: Histogram) void {
        self.values.deinit();
    }

    pub fn add(self: *Histogram, index: usize, amount: usize) !void {
        if (self.values.items.len <= index) {
            const old_len = self.values.items.len;
            try self.values.resize(index + 1);
            for (self.values.items[old_len..]) |*v| v.* = 0;
        }

        self.values.items[index] += amount;
    }

    pub fn dump(self: Histogram) void {
        for (self.values.items) |amount, index| {
            if (amount != 0) {
                std.log.info("{}: {}", .{ index, amount });
            }
        }
    }
};
