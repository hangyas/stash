const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

pub const NodeKind = enum(u1) { node, leaf };

const Storage = @import("./storage.zig").Storage;

// internal struct to represent where to find a key
pub const Position = struct { node: Node, idx: u16 };

//
// HEADER FORMAT:
// | kind(1) | key_count(2) | child_pointers((key_count + 1) * 8) | offsets(key_count * 2) | kv pairs... |
// kv pair:
// | key_length(8) | value_length(8) | key(key_length * 8) | value(value_length * 8) |
//
pub const header_size = 1 + 2;
pub const kv_header_size = 4;

fn panicOutOfRange() noreturn {
    @panic("out of range");
}

pub const Node = struct {
    // always having key_count() + 1 childrend
    //
    //   B D
    // ┌┘ │ └┐
    // A  C  E
    //
    // A < B < C < D < E

    // shouldn't know how to read/write itself from disk

    data: []u8,

    pub fn init(allocator: Allocator) Node {
        // todo page size properly
        const page_size = 512;
        const data: []u8 = allocator.alloc(u8, page_size) catch unreachable;
        @memset(data, 0);

        return Node{
            .data = data
        };
    }

    pub fn kind(this: Node) NodeKind {
        return @enumFromInt(this.data[0]);
    }

    pub fn key_count(this: Node) u16 {
        return std.mem.readInt(u16, this.data[1..3], .big);
    }

    pub fn child_count(self: Node) u16 {
        return self.key_count() + 1;
    }

    //
    // child pointers
    //

    // position of a pointer in the data array
    // checks for limits
    pub fn ptr_pos(this: Node, idx: u16) u16 {
        if (idx < 0 or idx >= this.child_count()) {
            panicOutOfRange();
        }

        return header_size + 8 * idx;
    }

    pub fn get_ptr(this: Node, idx: u16) u64 {
        const p = this.ptr_pos(idx);
        // slice the slice, so the size is known at compile time
        const slice: *[8]u8 = this.data[p .. p + 8][0..8];
        return std.mem.readInt(u64, slice, .big);
    }

    //
    // key value pair offsets
    //

    // ith cell stores the offset position of i+1
    // 0 is hardcoded to 0

    // position of an offset in the data array
    // checks for limits
    // only call from set/get offset
    pub fn offset_pos(this: Node, idx: u16) u16 {
        if (idx < 0 or idx >= this.key_count()) {
            panicOutOfRange();
        }

        return header_size + 8 * this.child_count() + 2 * idx;
    }

    pub fn get_offset(this: Node, idx: u16) u16 {
        if (idx == 0) {
            return 0;
        } else {
            const p = this.offset_pos(idx - 1);

            std.debug.print("read p: {d}\n", .{p});

            const slice: *[2]u8 = this.data[p .. p + 2][0..2];
            return std.mem.readInt(u16, slice, .big);
        }
    }


    //
    // Key-Value pairs
    //

    // position of a kv pair in the data array
    // checks for limits
    pub fn kv_pos(this: Node, idx: u16) u16 {
        if (idx < 0 or idx >= this.key_count()) {
            panicOutOfRange();
        }

        const offset = this.get_offset(idx);

        std.debug.print("{d}, {d}, {d}, {d}\n", .{header_size, this.child_count(), this.key_count(), offset});
        return header_size + 8 * this.child_count() + 2 * this.key_count() + offset;
    }

    pub fn get_kv_slice(this: Node, idx: u16) []u8 {
        const p = this.kv_pos(idx);

        // key_length is 2 bytes
        const key_length_slice: *[2]u8 = this.data[p .. p + 2][0..2];
        const key_length = std.mem.readInt(u16, key_length_slice, .big);

        // val_length is 2 bytes
        const val_length_slice: *[2]u8 = this.data[p + 2 .. p + 4][0..2];
        const val_length = std.mem.readInt(u16, val_length_slice, .big);

        return this.data[p .. p + 4 + key_length + val_length];
    }

    pub fn get_key(this: Node, idx: u16) []u8 {
        const p = this.kv_pos(idx);

        const slice: *[2]u8 = this.data[p .. p + 2][0..2];
        const key_length = std.mem.readInt(u16, slice, .big);
        return this.data[p + 4 .. p + 4 + key_length];
    }


    pub fn get_value(this: Node, idx: u16) []u8 {
        const p = this.kv_pos(idx);
        // key_length is 2 bytes
        const key_length_slice: *[2]u8 = this.data[p .. p + 2][0..2];
        const key_length = std.mem.readInt(u16, key_length_slice, .big);
        // val_length is 2 bytes
        const val_length_slice: *[2]u8 = this.data[p + 2 .. p + 4][0..2];
        const val_length = std.mem.readInt(u16, val_length_slice, .big);
        return this.data[p + 4 + key_length .. p + 4 + key_length + val_length];
    }


    pub fn to_string(self: Node, allocator: Allocator, storage: Storage) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        try buffer.appendSlice("{ ");

        var i: u16 = 0;
        while (i < self.key_count()) : (i += 1) {
            if (self.kind() == .node) {
                var buf: [64]u8 = undefined;
                try buffer.appendSlice(try std.fmt.bufPrint(&buf, "[0x{x}]", .{self.get_ptr(i)}));

                // const child = storage.node_from_ptr(self.get_ptr(i));
                // try buffer.appendSlice(try child.to_string(allocator, storage));
            }

            try buffer.append('"');
            try buffer.appendSlice(self.get_key(i));
            // try buffer.appendSlice("\": \"");
            // try buffer.appendSlice(try self.get_value(i));
            try buffer.appendSlice("\", ");
        }

        if (self.kind() == .node) {
            var buf: [64]u8 = undefined;
            try buffer.appendSlice(try std.fmt.bufPrint(&buf, "[0x{x}]", .{self.get_ptr(i)}));

            const child = storage.node_from_ptr(self.get_ptr(i));
            try buffer.appendSlice(try child.to_string(allocator, storage));
        }

        try buffer.appendSlice("}");

        return buffer.toOwnedSlice();
    }
};


test "enum ordinal value" {
    try testing.expect(@intFromEnum(NodeKind.node) == 0);
    try testing.expect(@intFromEnum(NodeKind.leaf) == 1);
}

test "kind" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const node = Node.init(allocator);
    const data = node.data;

    data[0] = 1;
    try testing.expect(node.kind() == .leaf);
    data[0] = 0;
    try testing.expect(node.kind() == .node);
}


test "key_count" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const node = Node.init(allocator);
    const data = node.data;

    // key count is 2 bytes, we check them directly
    data[1] = 0;
    data[2] = 1;
    try testing.expect(node.key_count() == 1);
    data[2] = 0;
    try testing.expect(node.key_count() == 0);
    data[2] = 18;
    try testing.expect(node.key_count() == 18);
}

test "child pointers" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const node = Node.init(allocator);
    const data = node.data;

    // key count
    data[1] = 0;
    data[2] = 5;

    const p1 = header_size;
    const slice1: *[8]u8 = data[p1 .. p1 + 8][0..8];
    std.mem.writeInt(u64, slice1, 16, .big);
    const p2 = header_size + 3 * 8;
    const slice2: *[8]u8 = data[p2 .. p2 + 8][0..8];
    std.mem.writeInt(u64, slice2, 17, .big);

    try testing.expectEqual(@as(u64, 16), node.get_ptr(0));
    try testing.expectEqual(@as(u64, 17), node.get_ptr(3));
}

test "kv offsets" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const node = Node.init(allocator);
    const data = node.data;

    const child_count = 4; // = key_count + 1
    const idx = 2;

    const p1 = header_size + 8 * child_count + 2 * (idx - 1);
    const slice1: *[2]u8 = data[p1 .. p1 + 2][0..2];
    std.mem.writeInt(u16, slice1, 13, .big);

    // key count
    data[1] = 0;
    data[2] = 3;

    try testing.expectEqual(@as(u16, 3), node.key_count());
    try testing.expectEqual(@as(u16, 13), node.get_offset(idx));
}

test "get_key" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const node = Node.init(allocator);
    const data = node.data;

    const key_pos = 147;

    const child_count = 4; // = key_count + 1
    const idx = 2;

    const p1 = header_size + 8 * child_count + 2 * (idx - 1);
    const slice1: *[2]u8 = data[p1 .. p1 + 2][0..2];
    std.mem.writeInt(u16, slice1, key_pos, .big);

    // key count
    data[1] = 0;
    data[2] = 3;

    // key
    const key = "test-key";
    const key_len: u16 = @intCast(key.len);

    const key_len_slice: *[2]u8 = data[188 .. 190][0..2];
    std.mem.writeInt(u16, key_len_slice, key_len, .big);
    const key_slice: []u8 = data[192 .. 192 + key_len][0..key_len];
    @memcpy(key_slice, key);

    try testing.expectEqual(@as(u16, 3), node.key_count());
    try testing.expectEqual(key_pos, node.get_offset(idx));
    try testing.expectEqualStrings(key, node.get_key(idx));
}