const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

const Node = @import("./node.zig").Node;
const kv_header_size = @import("./node.zig").kv_header_size;
const NodeKind = @import("./node.zig").NodeKind;
const Storage = @import("./storage.zig").Storage;
const KVPair = @import("./kv_pair.zig");

const NODE_SIZE = 4;

pub const NodeBuilder = struct {
    // functions to build 1 single node, should not know about other nodes and their relations(?)

    // node that is under construction
    // root: Node,

    // needed for creating new children
    allocator: Allocator,
    storage: Storage,

    // keep track of changes
    dirty_children: std.ArrayList(NodeBuilder),
    orphaned_nodes: std.ArrayList(Node),

    // from the format of nodes
    // | kind(1) | key_count(2) | child_pointers((key_count + 1) * 8) | offsets(key_count * 2) | kv pairs... |
    kind: NodeKind,
    // key_count: u16,

    child_ptrs: []u64,

    // | key_length(8) | value_length(8) | key(key_length * 8) | value(value_length * 8) |
    // each kv pair is stored as a separate byte slice. The kv pairs themselves are the byte slices as in the final format
    kv_pairs: [][]u8,
    // const mat3x2 = [_][2]u32{
    //     [_]u32{ 1, 2 },
    //     [_]u32{ 3, 4 },
    //     [_]u32{ 5, 6 },
    // };

    // no need to store offsets, calculate at build()

    pub fn init(storage: Storage, allocator: Allocator) NodeBuilder {
        const kv_pairs: [][]u8 = allocator.alloc([]u8, 0) catch unreachable;
        const child_ptrs: []u64 = allocator.alloc(u64, 0) catch unreachable;

        return .{ .dirty_children = std.ArrayList(NodeBuilder).init(allocator), .orphaned_nodes = std.ArrayList(Node).init(allocator), .allocator = allocator, .storage = storage, .kind = .leaf, .kv_pairs = kv_pairs, .child_ptrs = child_ptrs };
    }

    pub fn copy(self: NodeBuilder, other_node: Node) void {
        self.allocator.free(self.kv_pairs);
        self.allocator.free(self.child_ptrs);

        self.kv_pairs = self.allocator.alloc([]u8, other_node.key_count());
        self.child_ptrs = self.allocator.alloc(u64, other_node.child_count());

        for (0..other_node.key_count()) |i| {
            self.kv_pairs[i] = other_node.get_kv_slice(i);
        }

        for (0..other_node.child_count()) |i| {
            self.child_ptrs[i] = other_node.get_ptr(i);
        }
    }

    // todo storage would only be needed when persisting to disk(?)
    pub fn build(self: NodeBuilder) Node {
        const page_size = 512;
        const header_size = 3;

        const data: []u8 = self.allocator.alloc(u8, page_size) catch unreachable;
        @memset(data, 0);

        // write bytes based on the memory format of Node

        // kind
        data[0] = @intFromEnum(self.kind);
        // key count
        std.mem.writeInt(u16, data[1..3], @intCast(self.kv_pairs.len), .big);

        // child pointers
        for (self.child_ptrs, 0..) |child_ptr, idx| {
            const p = header_size + 8 * idx;
            const slice: *[8]u8 = data[p .. p + 8][0..8];
            std.mem.writeInt(u64, slice, @intCast(child_ptr), .big);
        }

        // offsets and kv pairs

        var prev_offset: u16 = 0;
        for (self.kv_pairs, 0..) |kv_pair, idx| {
            // const offset = prev_offset + KVPair.kv_header_size + KVPair.get_key_len(kv_pair) + KVPair.get_value_len(kv_pair);
            const offset: u16 = prev_offset + @as(u16, @intCast(kv_pair.len));

            if (idx > 0) {
                const p = header_size + 8 * self.child_ptrs.len + 2 * idx;
                const slice: *[2]u8 = data[p .. p + 2][0..2];

                std.mem.writeInt(u16, slice, offset, .big);

                prev_offset = offset;
            }

            const kv_pos = header_size + 8 * self.child_ptrs.len + 2 * self.kv_pairs.len + offset;

            const slice = data[kv_pos .. kv_pos + kv_pair.len];
            @memcpy(slice, kv_pair);
        }

        return Node{ .data = data };
    }

    pub fn createChildBuilder(self: *NodeBuilder) NodeBuilder {
        // TODO
        const builder = NodeBuilder.init(self.storage, self.allocator);
        self.dirty_children.append(builder) catch @panic("memory error at dirty_childent.append");
        return builder;
    }

    // basic methods

    pub fn setKind(this: *NodeBuilder, new_kind: NodeKind) void {
        this.kind = new_kind;
    }

    // pub fn setHeaders(this: NodeBuilder, new_kind: NodeKind, new_key_count: u16) void {
    //     this.root.data[0] = @intFromEnum(new_kind);
    //     std.mem.writeInt(u16, this.root.data[1..3], new_key_count, .big);
    // }

    pub fn setPtr(this: NodeBuilder, idx: u16, val: u64) void {
        // const p = this.root.ptr_pos(idx);
        // // slice the slice, so the size is known at compile time
        // const slice: *[8]u8 = this.root.data[p .. p + 8][0..8];
        // return std.mem.writeInt(u64, slice, val, .big);

        this.child_ptrs[idx] = val;
    }

    // pub fn setOffset(this: NodeBuilder, idx: u16, val: u16) void {
    //     if (idx > 0) {
    //         const p = this.root.offset_pos(idx - 1);
    //
    //         std.debug.print("write p: {d}\n", .{p});
    //
    //         const slice: *[2]u8 = this.root.data[p .. p + 2][0..2];
    //         return std.mem.writeInt(u16, slice, val, .big);
    //     }
    // }

    pub fn isFull(self: NodeBuilder) bool {
        return self.kv_pairs.len == 2 * NODE_SIZE - 1;
    }

    // higher level

    fn appendKV(this: *NodeBuilder, ptr: u64, key: []const u8, val: []const u8) void {
        // append pointer
        // this.setPtr(idx, ptr);
        const old_ptrs = this.child_ptrs;
        this.child_ptrs = this.allocator.alloc(u64, old_ptrs.len + 1) catch unreachable;
        @memcpy(this.child_ptrs[0 .. old_ptrs.len], old_ptrs);
        this.child_ptrs[old_ptrs.len] = ptr;
        this.allocator.free(old_ptrs);

        // prepare the kv_pair
        const key_len: u16 = @intCast(key.len);
        const val_len: u16 = @intCast(val.len);

        const kv_slice = this.allocator.alloc(u8, 8 + 8 + key_len + val_len) catch unreachable;

        const key_len_slice: *[2]u8 = kv_slice[0..2];
        const val_len_slice: *[2]u8 = kv_slice[2..4];
        std.mem.writeInt(u16, key_len_slice, key_len, .big);
        std.mem.writeInt(u16, val_len_slice, val_len, .big);

        const key_slice: []u8 = kv_slice[4 .. 4 + key_len][0..key_len];
        @memcpy(key_slice, key);
        const val_slice: []u8 = kv_slice[4 + key_len .. 4 + key_len + val_len][0..val_len];
        @memcpy(val_slice, val);

        // append kv pair
        const old_kv_pairs = this.kv_pairs;
        this.kv_pairs = this.allocator.alloc([]u8, old_kv_pairs.len + 1) catch unreachable;
        @memcpy(this.kv_pairs[0 .. old_kv_pairs.len], old_kv_pairs);
        this.kv_pairs[old_kv_pairs.len] = kv_slice;
        this.allocator.free(old_kv_pairs);

        // this.setOffset(idx + 1, this.root.get_offset(idx) + key_len + val_len + kv_header_size);
        // const p = this.root.kv_pos(idx);
        //
        // const key_len_slice: *[2]u8 = this.root.data[p .. p + 2][0..2];
        // const val_len_slice: *[2]u8 = this.root.data[p + 2 .. p + 4][0..2];
        // std.mem.writeInt(u16, key_len_slice, key_len, .big);
        // std.mem.writeInt(u16, val_len_slice, val_len, .big);
        //
        // const key_slice: []u8 = this.root.data[p + 4 .. p + 4 + key_len][0..key_len];
        // @memcpy(key_slice, key);
        // const val_slice: []u8 = this.root.data[p + 4 + key_len .. p + 4 + key_len + val_len][0..val_len];
        // @memcpy(val_slice, val);
    }

    //
    // bulk operations
    //

    pub fn copyRange(self: NodeBuilder, old: Node, idx_new: u16, idx_old: u16, n: u16) void {
        var i: u16 = 0;
        while (i < n) : (i += 1) {
            const key = old.get_key(idx_old + i);
            const val = old.get_value(idx_old + i);
            self.appendKV(idx_new + i, 0, key, val);
        }
    }

    pub fn copyLeftPtrs(new: NodeBuilder, old: Node, idx_new: u16, idx_old: u16, n: u16) void {
        var i: u16 = 0;
        while (i < n) : (i += 1) {
            const ptr = old.get_ptr(idx_old + i);
            new.setPtr(idx_new + i, ptr);
        }
    }

    pub fn copyRightPtrs(new: NodeBuilder, old: Node, idx_new: u16, idx_old: u16, n: u16) void {
        var i: u16 = 0;
        while (i < n) : (i += 1) {
            const ptr = old.get_ptr(idx_old + i + 1);
            new.setPtr(idx_new + i + 1, ptr);
        }
    }

    // insert a KV in the middle: shift KVs from the right of idx to have enough space, then insert KV
    // pub fn shiftAndInsert(self: NodeBuilder, idx: u16, key: []const u8, val: []const u8) void {
    //     const key_len: u16 = @intCast(key.len);
    //     const val_len: u16 = @intCast(val.len);
    //
    //     const shift = key_len + val_len + kv_header_size;
    //
    //     // shift KV pairs
    //     var i: u16 = self.root.key_count();
    //     while (i > idx) : (i -= 1) {
    //         const size = self.root.get_offset(i - 1) - self.root.get_offset(i - 2);
    //
    //         const old_pos = self.root.get_offset(i - 1);
    //         const new_pos = old_pos + shift;
    //         self.setOffset(i, new_pos);
    //
    //         std.mem.copyBackwards(u8, self.root.data[new_pos .. new_pos + size][0..size], self.root.data[old_pos .. old_pos + size][0..size]);
    //
    //         self.setPtr(i, self.root.get_ptr(i - 1));
    //     }
    //
    //     self.setHeaders(self.root.kind(), self.root.key_count() + 1);
    //
    //     const p = self.root.kv_pos(idx);
    //
    //     const key_len_slice: *[2]u8 = self.root.data[p .. p + 2][0..2];
    //     const val_len_slice: *[2]u8 = self.root.data[p + 2 .. p + 4][0..2];
    //     std.mem.writeInt(u16, key_len_slice, key_len, .big);
    //     std.mem.writeInt(u16, val_len_slice, val_len, .big);
    //
    //     const key_slice: []u8 = self.root.data[p + 4 .. p + 4 + key_len][0..key_len];
    //     @memcpy(key_slice, key);
    //     //std.mem.copyForward(u8, key_slice, key);
    //     const val_slice: []u8 = self.root.data[p + 4 + key_len .. p + 4 + key_len + val_len][0..val_len];
    //     @memcpy(val_slice, val);
    //
    //     std.debug.print("set {d} offset to {d}\n", .{ idx + 1, self.root.get_offset(idx) + key_len + val_len + kv_header_size });
    //     self.setOffset(idx + 1, self.root.get_offset(idx) + key_len + val_len + kv_header_size);
    // }
};

// test "set_header" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//
//     const str = Storage.createInMemory(allocator, 512);
//     const builder = NodeBuilder.init(str, allocator);
//
//     builder.setHeaders(.node, 5);
//     try testing.expect(builder.root.data[0] == 0);
//     // key count is 2 bytes, we check them directly
//     try testing.expect(builder.root.data[1] == 0);
//     try testing.expect(builder.root.data[2] == 5);
// }
//
test "appendKV" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const str = Storage.createInMemory(allocator, 512);
    var builder = NodeBuilder.init(str, allocator);

    builder.setKind(.node);

    builder.appendKV(0, "hey", "val-longer");

    const node = builder.build();

    std.debug.print("node: {s}\n", .{try node.to_string(allocator, str)});

    try testing.expectEqualStrings("hey", node.get_key(0));
    try testing.expectEqual(3, node.get_key(0).len);
    try testing.expectEqualStrings("val-longer", node.get_value(0));
    try testing.expectEqual(10, node.get_value(0).len);

    // builder.appendKV(1, 0, "next-key", "val-other");
    // try testing.expectEqualStrings("hey", builder.root.get_key(0));
    // try testing.expectEqualStrings("val-longer", builder.root.get_value(0));
    // try testing.expectEqualStrings("next-key", builder.root.get_key(1));
    // try testing.expectEqualStrings("val-other", builder.root.get_value(1));
    //
    // builder.appendKV(2, 0, "next-next-key", "val2");
    // try testing.expectEqualStrings("hey", builder.root.get_key(0));
    // try testing.expectEqualStrings("val-longer", builder.root.get_value(0));
    // try testing.expectEqualStrings("next-key", builder.root.get_key(1));
    // try testing.expectEqualStrings("val-other", builder.root.get_value(1));
    // try testing.expectEqualStrings("next-next-key", builder.root.get_key(2));
    // try testing.expectEqualStrings("val2", builder.root.get_value(2));
}
//
// test "setOffset" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//
//     const str = Storage.createInMemory(allocator, 512);
//     const builder = NodeBuilder.init(str, allocator);
//
//     builder.setHeaders(.leaf, 5);
//     // TODO changing the key count changes to position of values in the array -> pretty sure this is causing bugs now
//
//     // won't set 0
//     builder.setOffset(0, 101);
//     try testing.expectEqual(0, builder.root.get_offset(0));
//
//     builder.setOffset(1, 101);
//     try testing.expectEqual(101, builder.root.get_offset(1));
//     builder.setOffset(2, 102);
//     try testing.expectEqual(102, builder.root.get_offset(2));
//     builder.setOffset(3, 103);
//     try testing.expectEqual(103, builder.root.get_offset(3));
// }
