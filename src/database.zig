const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

const TreeKind = enum(u1) { node, leaf };

pub const DBError = error{ IndexOutOfRange, ChildIndexOutOfRange };

//
// HEADER FORMAT:
// | kind(1) | key_count(2) | child_pointers((key_count + 1) * 8) | offsets(key_count * 2) | kv pairs... |
// kv pair:
// | key_length(8) | value_length(8) | key(key_length * 8) | value(value_length * 8) |
//

const header_size = 1 + 2;

// wraps byte arrays
// can be passed around directly, a []u8 is { .ptr, .len }, the actual bytes leave on the heap (at root.ptr)
// TODO this should be BNode
pub const BTree = struct {
    // always having key_count() + 1 childrend
    //
    //   B D
    // ┌┘ │ └┐
    // A  C  E
    //
    // A < B < C < D < E

    root: []u8,

    fn kind(this: BTree) TreeKind {
        return @enumFromInt(this.root[0]);
    }

    fn key_count(this: BTree) u16 {
        return std.mem.readInt(u16, this.root[1..3], .big);
    }

    fn child_count(self: BTree) u16 {
        return self.key_count() + 1;
    }

    fn set_headers(this: BTree, new_kind: TreeKind, new_key_count: u16) void {
        this.root[0] = @intFromEnum(new_kind);
        std.mem.writeInt(u16, this.root[1..3], new_key_count, .big);
    }

    //
    // child pointers
    //

    // position of a pointer in the data array
    // checks for limits
    fn ptr_pos(this: BTree, idx: u16) !u16 {
        if (idx < 0 or idx >= this.child_count()) {
            return DBError.ChildIndexOutOfRange;
        }

        return header_size + 8 * idx;
    }

    fn get_ptr(this: BTree, idx: u16) !u64 {
        const p = try this.ptr_pos(idx);
        // slice the slice, so the size is known at compile time
        const slice: *[8]u8 = this.root[p .. p + 8][0..8];
        return std.mem.readInt(u64, slice, .big);
    }

    fn set_ptr(this: BTree, idx: u16, val: u64) !void {
        const p = try this.ptr_pos(idx);
        // slice the slice, so the size is known at compile time
        const slice: *[8]u8 = this.root[p .. p + 8][0..8];
        return std.mem.writeInt(u64, slice, val, .big);
    }

    //
    // key value pair offsets
    //

    // ith cell stores the offset position of i+1
    // 0 is hardcoded to 0

    // position of an offset in the data array
    // checks for limits
    // only call from set/get offset
    fn offset_pos(this: BTree, idx: u16) !u16 {
        if (idx < 0 or idx >= this.key_count()) {
            return DBError.IndexOutOfRange;
        }

        return header_size + 8 * this.child_count() + 2 * idx;
    }

    fn get_offset(this: BTree, idx: u16) !u16 {
        if (idx == 0) {
            return 0;
        } else {
            const p = try this.offset_pos(idx - 1);
            const slice: *[2]u8 = this.root[p .. p + 2][0..2];
            return std.mem.readInt(u16, slice, .big);
        }
    }

    fn set_offset(this: BTree, idx: u16, val: u16) !void {
        if (idx > 0) {
            const p = try this.offset_pos(idx - 1);
            const slice: *[2]u8 = this.root[p .. p + 2][0..2];
            return std.mem.writeInt(u16, slice, val, .big);
        }
    }

    //
    // Key-Value pairs
    //

    // position of a kv pair in the data array
    // checks for limits
    fn kv_pos(this: BTree, idx: u16) !u16 {
        if (idx < 0 or idx >= this.key_count()) {
            return DBError.IndexOutOfRange;
        }

        const offset = try this.get_offset(idx);
        return header_size + 8 * this.child_count() + 2 * this.key_count() + offset;
    }

    fn get_key(this: BTree, idx: u16) ![]u8 {
        const p = try this.kv_pos(idx);
        const slice: *[2]u8 = this.root[p .. p + 2][0..2];
        const key_length = std.mem.readInt(u16, slice, .big);
        return this.root[p + 4 .. p + 4 + key_length];
    }

    fn get_value(this: BTree, idx: u16) ![]u8 {
        const p = try this.kv_pos(idx);
        // key_length is 2 bytes
        const key_length_slice: *[2]u8 = this.root[p .. p + 2][0..2];
        const key_length = std.mem.readInt(u16, key_length_slice, .big);
        // val_length is 2 bytes
        const val_length_slice: *[2]u8 = this.root[p + 2 .. p + 4][0..2];
        const val_length = std.mem.readInt(u16, val_length_slice, .big);
        return this.root[p + 4 + key_length .. p + 4 + key_length + val_length];
    }

    //
    // OTHER
    //

    // fn node_size(this: BTree) !u16 {
    //     return this.kv_pos(this.key_count());
    // }

    // returns the first kid node whose range intersect the key (kid[i] <= key
    // fn node_lookup_less_than(this: BTree, key: []u8) !u18 {
    //     const nkeys = this.key_count();
    //     var found = 0;
    //     // the first key is a copy from the parent node, so it's always <= key
    //     for (1..nkeys) |i| {
    //         switch (std.mem.order(u8, this.get_key(i), key)) {
    //             .lt => found = i,
    //             .eq => continue,
    //             .gt => continue,
    //         }
    //     }
    //
    //     return found;
    // }

    //
    // append, delete
    //

    // internal functions for copying into new node

    fn append_kv(this: BTree, idx: u16, ptr: u64, key: []const u8, val: []const u8) !void {
        const key_len: u16 = @intCast(key.len);
        const val_len: u16 = @intCast(val.len);

        try this.set_ptr(idx, ptr);
        const kv_header_size = 4;
        try this.set_offset(idx + 1, (try this.get_offset(idx)) + key_len + val_len + kv_header_size);
        const p = try this.kv_pos(idx);

        const key_len_slice: *[2]u8 = this.root[p .. p + 2][0..2];
        const val_len_slice: *[2]u8 = this.root[p + 2 .. p + 4][0..2];
        std.mem.writeInt(u16, key_len_slice, key_len, .big);
        std.mem.writeInt(u16, val_len_slice, val_len, .big);

        const key_slice: []u8 = this.root[p + 4 .. p + 4 + key_len][0..key_len];
        @memcpy(key_slice, key);
        //std.mem.copyForward(u8, key_slice, key);
        const val_slice: []u8 = this.root[p + 4 + key_len .. p + 4 + key_len + val_len][0..val_len];
        @memcpy(val_slice, val);
        //std.mem.copyForward(u8, val_slice, val);
    }

    // fn leaf_insert(new: BTree, old: BTree, idx: u16, key: [:0]const u8, val: [:0]const u8) !void {
    //     new.set_headers(.leaf, old.key_count() + 1);
    //     try node_copy_range(new, old, 0, 0, idx);
    //     try new.append_kv(idx, 0, key, val);
    //     try node_copy_range(new, old, idx + 1, idx, old.key_count() - idx);
    // }

    // -- starting from here, functions are my own implementation based on the algorithm book --
    // did this, because the build your own database book was confusing

    // ptr[i] is elements < keys[i]

    // splits the idx child and moves the pivot item into the parent (current node)
    // to keep stuff atomic, no in-place mutation is done, 3 new nodes are returned
    //
    //      ... N W ...            ... N S X ...
    //           │                  ┌───┘ └───┐
    //           │                  │         │
    //           ▼                  ▼         ▼
    //     P Q R S T U V          P Q R     T U V
    //    │ │ │ │ │ │ │ │        │ │ │ │   │ │ │ │
    //    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼        ▼ ▼ ▼ ▼   ▼ ▼ ▼ ▼
    //    T...                   T...
    //
    // it's possible for the parent to have 0 keys and 1 child before split
    //
    fn split_child(self: BTree, allocator: Allocator, idx: u16) ![3]BTree {
        // note: first key is duplicate from parent
        const child_ptr = try self.get_ptr(idx);
        const child: BTree = node_from_ptr(child_ptr);
        const pivot_idx = child.key_count() / 2;

        // todo is the rounding of size ok like this?
        // 1 goes up to the parent, others are split
        const left_child_size = pivot_idx;
        // pivot element is moved to the parents -> +/- 1
        const right_child_size = child.key_count() - pivot_idx - 1;
        const parent_size = self.key_count() + 1;

        const left = try create_node(allocator);
        const right = try create_node(allocator);
        // new parent = updated copy of self
        const parent = try create_node(allocator);

        // creating and initializing the new child
        left.set_headers(child.kind(), left_child_size);
        right.set_headers(child.kind(), right_child_size);
        parent.set_headers(self.kind(), parent_size);

        // left
        try node_copy_range(left, child, 0, 0, left_child_size);
        // copy the last right pointer too
        try node_copy_left_ptrs(left, child, 0, 0, left_child_size + 1);

        // right
        try node_copy_range(right, child, 0, pivot_idx + 1, right_child_size);
        // copy the last right pointer too
        try node_copy_left_ptrs(left, child, 0, pivot_idx + 1, right_child_size + 1);

        // parent
        try node_copy_range(parent, self, 0, 0, idx);
        // todo remove ptr from append_kv and set them separately
        try parent.append_kv(idx, 0, try child.get_key(pivot_idx), try child.get_value(pivot_idx));
        if (self.key_count() - idx > 0) {
            try node_copy_range(parent, self, idx + 1, idx, self.key_count() - idx - 1);
        }
        // parent pointers
        try node_copy_left_ptrs(parent, self, 0, 0, idx);
        try parent.set_ptr(idx, node_to_ptr(left));
        try parent.set_ptr(idx + 1, node_to_ptr(right));
        if (self.key_count() - idx > 2) {
            try node_copy_right_ptrs(parent, self, idx + 2, idx + 1, self.key_count() - idx - 2);
        }

        return .{ parent, left, right };
    }

    // internal struct to represent where to find a key
    const Position = struct { node: BTree, idx: u16 };

    // finds the position (node + idx) recursively in a tree
    fn find_key_position(self: BTree, key: []const u8) !?Position {
        var i: u16 = 0;
        while (i < self.key_count() and std.mem.order(u8, try self.get_key(i), key) == .lt) {
            i += 1;
        }

        if (i < self.key_count() and std.mem.order(u8, key, try self.get_key(i)) == .eq) {
            return Position{ .node = self, .idx = i };
        }

        // TODO 0 pointers shouldn't be there or handled properly
        if (self.kind() == .node and try self.get_ptr(i) != 0) {
            const node: BTree = node_from_ptr(try self.get_ptr(i));
            return node.find_key_position(key);
        }

        return null;
    }

    // TODO have a separate mutable temporal Node and a final unmutable node type

    // TODO better error handling
    fn insert(self: BTree, allocator: Allocator, key: []const u8, value: []const u8) anyerror!BTree {
        // todo move constant
        const t = 4;
        // when full, we need to create a new root
        if (self.key_count() == 2 * t - 1) {
            var new_root = try create_node(allocator);

            // has 0 keys, but 1 child
            new_root.set_headers(.node, 0);
            try new_root.set_ptr(0, node_to_ptr(self));

            // todo this steps creates a new node from an already freshly created node -> optimize
            const splitted = try new_root.split_child(allocator, 0);

            new_root = splitted[0];
            const r = new_root.insert_into_non_full_tree(allocator, key, value);
            return r;
        } else {
            std.debug.print("insert {s} into non-full\n", .{key});
            return self.insert_into_non_full_tree(allocator, key, value);
        }
    }

    fn insert_into_non_full_tree(self: BTree, allocator: Allocator, key: []const u8, value: []const u8) !BTree {
        const new_node = try create_node(allocator);
        new_node.set_headers(self.kind(), self.key_count() + 1);
        if (self.kind() == .leaf) {
            var i: u16 = 0;

            while (i < self.key_count() and std.mem.order(u8, try self.get_key(i), key) == .lt) {
                i += 1;
            }

            try node_copy_range(new_node, self, 0, 0, i);
            try new_node.append_kv(i, 0, key, value);
            try node_copy_range(new_node, self, i + 1, i, self.key_count() - i);

            return new_node;
        } else {
            var i: u16 = 0;
            while (i < self.key_count() and std.mem.order(u8, try self.get_key(i), key) == .lt) {
                i += 1;
            }

            // it's an internal node so pointers aren't empty
            var child: BTree = node_from_ptr(try self.get_ptr(i));
            // std.debug.print("insert {s} into child {s}\n", .{key, try child.to_string(allocator)});

            // todo make self mutable, or return a new self in everycase
            var new_self = self;

            // todo move constant
            const t = 4;
            // if it's full then split it
            if (child.key_count() > 2 * t - 1) {
                // std.debug.print("child is full, splitting?\n", .{});
                const updated_nodes = try new_self.split_child(allocator, i);
                new_self = updated_nodes[0];

                // check if it went to left or right
                if (std.mem.order(u8, try new_self.get_key(i), key) == .lt) {
                    i += 1;
                }
                child = node_from_ptr(try new_self.get_ptr(i));
            }

            child = try child.insert(allocator, key, value);
            try new_self.set_ptr(i, node_to_ptr(child));
            return new_self;
        }
    }

    fn to_string(self: BTree, allocator: Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        try buffer.appendSlice("{ ");

        var i: u16 = 0;
        while (i < self.key_count()) : (i += 1) {
            if (self.kind() == .node) {
                var buf: [64]u8 = undefined;
                try buffer.appendSlice(try std.fmt.bufPrint(&buf, "[0x{x}]", .{try self.get_ptr(i)}));

                const child: BTree = node_from_ptr(try self.get_ptr(i));
                try buffer.appendSlice(try child.to_string(allocator));
            }

            try buffer.append('"');
            try buffer.appendSlice(try self.get_key(i));
            // try buffer.appendSlice("\": \"");
            // try buffer.appendSlice(try self.get_value(i));
            try buffer.appendSlice("\", ");
        }

        if (self.kind() == .node) {
            var buf: [64]u8 = undefined;
            try buffer.appendSlice(try std.fmt.bufPrint(&buf, "[0x{x}]", .{try self.get_ptr(i)}));

            const child: BTree = node_from_ptr(try self.get_ptr(i));
            try buffer.appendSlice(try child.to_string(allocator));
        }

        try buffer.appendSlice("}");

        return buffer.toOwnedSlice();
    }
};

// TODO move constants
const NODE_SIZE = 1024;

fn create_node(allocator: Allocator) !BTree {
    // todo figure out alloc size
    const data: []u8 = try allocator.alloc(u8, NODE_SIZE);

    return BTree{
        .root = data,
    };
}

fn node_from_ptr(ptr_int: u64) BTree {
    const root_ptr: [*]u8 = @ptrFromInt(ptr_int);
    const root_slice: []u8 = root_ptr[0..NODE_SIZE];
    return BTree{ .root = root_slice };
}

fn node_to_ptr(tree: BTree) u64 {
    return @intFromPtr(tree.root.ptr);
}

fn node_copy_range(new: BTree, old: BTree, idx_new: u16, idx_old: u16, n: u16) !void {
    var i: u16 = 0;
    while (i < n) : (i += 1) {
        const key = try old.get_key(idx_old + i);
        const val = try old.get_value(idx_old + i);
        try new.append_kv(idx_new + i, 0, key, val);
    }
}

// fn node_copy_range_with_ptrs(new: BTree, old: BTree, idx_new: u16, idx_old: u16, n: u16) !void {
//     // TODO does node_copy_range needed seperately at all?
//     try node_copy_range(new, old, idx_new, idx_old, n);

fn node_copy_left_ptrs(new: BTree, old: BTree, idx_new: u16, idx_old: u16, n: u16) !void {
    var i: u16 = 0;
    while (i < n) : (i += 1) {
        const ptr = try old.get_ptr(idx_old + i);
        try new.set_ptr(idx_new + i, ptr);
    }
}

fn node_copy_right_ptrs(new: BTree, old: BTree, idx_new: u16, idx_old: u16, n: u16) !void {
    var i: u16 = 0;
    while (i < n) : (i += 1) {
        const ptr = try old.get_ptr(idx_old + i + 1);
        try new.set_ptr(idx_new + i + 1, ptr);
    }
}

// inserts many kids at idx..idx+kids.len and copy old
// result: old[0..idx] ++ kids ++ old[idx..]
// i'm not sure why are we inserting a list of kids
// fn node_replace_kid(allocator: Allocator, new: BTree, old: BTree, idx: u16, kids: []const BTree) !void {
//     const inc: u16 = @intCast(kids.len);
//     new.set_headers(.node, old.key_count() + inc);
//     try node_copy_range(new, old, 0, 0, idx);
//     for (0.., kids) |i, kid| {
//         // TODO destroy the old node?
//         const node = try create_node(allocator);
//         // TODO move this to a callback
//         const node_pointer: u64 = node_to_ptr(node);
//         const i_u16: u16 = @intCast(i);
//         try new.append_kv(idx + i_u16, node_pointer, try kid.get_key(0), "");
//     }
//     try node_copy_range(new, old, idx + inc, idx + 1, old.key_count() - idx - 1);
// }

// tests


test "learning about readInt and writeInt" {
    //const data: [2]u8 = .{0x0, 0x8};
    const r1: u16 = std.mem.readInt(u16, &.{ 0x0, 0x8 }, .big);
    //std.debug.print("result = {d}\n", .{r1});
    try testing.expect(8 == r1);
    const r2: u16 = std.mem.readInt(u16, &.{ 0x1, 0x0 }, .big);
    try testing.expect(256 == r2);
    //std.debug.print("result = {d}\n", .{r2});
    const r3: u16 = std.mem.readInt(u16, &.{ 0x1, 0x1 }, .big);
    try testing.expect(257 == r3);
    //std.debug.print("result = {d}\n", .{r3});

    var data: [4]u8 = .{ 0x0, 0x1, 0x1, 0x8 };
    const r4: u16 = std.mem.readInt(u16, data[0..2], .big);
    try testing.expect(1 == r4);
    //std.debug.print("result = {d}\n", .{r4});
    const r5: u16 = std.mem.readInt(u16, data[1..3], .big);
    try testing.expect(257 == r5);
    //std.debug.print("result = {d}\n", .{r5});
    const r6: u16 = std.mem.readInt(u16, data[2..4], .big);
    try testing.expect(264 == r6);
    //std.debug.print("result = {d}\n", .{r6});
    std.mem.writeInt(u16, data[1..3], 15, .big);

    try testing.expect(15 == std.mem.readInt(u16, data[1..3], .big));
}



// test "leaf_insert" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//
//     const data1: []u8 = try allocator.alloc(u8, 256);
//     const data2: []u8 = try allocator.alloc(u8, 256);
//
//     const tree1 = BTree{
//         .root = data1,
//     };
//     const tree2 = BTree{
//         .root = data2,
//     };
//
//     tree1.set_headers(TreeKind.node, 2);
//     try tree1.append_kv(0, 0, "hey", "val-longer");
//     try tree1.append_kv(1, 0, "next-key", "val-other");
//
//     try tree2.leaf_insert(tree1, 1, "inserted-key", "inserted-val");
//
//     try testing.expectEqualStrings("hey", try tree2.get_key(0));
//     try testing.expectEqualStrings("val-longer", try tree2.get_value(0));
//     try testing.expectEqualStrings("inserted-key", try tree2.get_key(1));
//     try testing.expectEqualStrings("inserted-val", try tree2.get_value(1));
//     try testing.expectEqualStrings("next-key", try tree2.get_key(2));
//     try testing.expectEqualStrings("val-other", try tree2.get_value(2));
// }

// test "node_replace_kid" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//
//     const tree1: BTree = try create_node(allocator);
//     tree1.set_headers(.node, 3);
//     try tree1.append_kv(0, 0, "key1", "val1");
//     try tree1.append_kv(1, 0, "key2", "val2");
//     try tree1.append_kv(2, 0, "key3", "val3");
//
//     const new_tree = try create_node(allocator);
//
//     const kid1 = try create_node(allocator);
//     kid1.set_headers(.node, 1);
//     try kid1.append_kv(0, 0, "key4", "val4");
//
//     const kid2 = try create_node(allocator);
//     kid2.set_headers(.node, 2);
//     try kid2.append_kv(0, 0, "key5", "val5");
//     try kid2.append_kv(1, 0, "key6", "val6");
//
//     const kids = [_]BTree{ kid1, kid2 };
//
//     try node_replace_kid(allocator, new_tree, tree1, 1, &kids);
//     //
//     //std.debug.print("{s}", .{try new_tree.get_key(0)});
//     //std.debug.print("{s}", .{try new_tree.get_key(1)});
//     //std.debug.print("{s}", .{try new_tree.get_key(2)});
//     //std.debug.print("{s}", .{try new_tree.get_key(3)});
//     //std.debug.print("{s}", .{try new_tree.get_key(4)});
//     //std.debug.print("{s}", .{try new_tree.get_key(5)});
//     //std.debug.print("{s}", .{try new_tree.get_key(6)});
//     //std.debug.print(new_tree.get_key(1));
//     //std.debug.print(new_tree.get_key(2));
//     //std.debug.print(new_tree.get_key(3));
//     //std.debug.print(new_tree.get_key(4));
//     //std.debug.print(new_tree.get_key(5));
//     //std.debug.print(new_tree.get_key(6));
// }

// test "split_child" {
//     // this is built incorrectly: ptr[i] should have elements < keys[i]
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//
//     const child = try create_node(allocator);
//     child.set_headers(.node, 3);
//     try child.append_kv(0, 0, "key1", "val1");
//     try child.append_kv(1, 0, "key2", "val2");
//     try child.append_kv(2, 0, "key3", "val3");
//
//     const tree1: BTree = try create_node(allocator);
//     tree1.set_headers(.node, 3);
//     try tree1.append_kv(0, node_to_ptr(child), "key4", "val4");
//     try tree1.append_kv(1, 0, "key5", "val5");
//     try tree1.append_kv(2, 0, "key6", "val6");
//
//     //std.debug.print("initial: {s}\n", .{try tree1.to_string(allocator)});
//
//     const new_nodes = try tree1.split_child(allocator, 0);
//
//     try testing.expectEqual(3, new_nodes.len);
//     std.debug.print("{s}\n", .{try new_nodes[0].to_string(allocator)});
//     //std.debug.print("left: {s}\n", .{try new_nodes[1].to_string(allocator)});
//     //std.debug.print("right: {s}\n", .{try new_nodes[2].to_string(allocator)});
//
//     try testing.expectEqualStrings("key1", try new_nodes[0].get_key(0));
//     try testing.expectEqualStrings("key2", try new_nodes[0].get_key(1));
//     try testing.expectEqualStrings("key4", try new_nodes[0].get_key(2));
//     try testing.expectEqualStrings("key5", try new_nodes[0].get_key(3));
//
//     try testing.expectEqualStrings("key1", try new_nodes[1].get_key(0));
//
//     try testing.expectEqualStrings("key2", try new_nodes[2].get_key(0));
//     try testing.expectEqualStrings("key3", try new_nodes[2].get_key(1));
// }

// test "split_child with even numbers" {
//     // this is built incorrectly: ptr[i] should have elements < keys[i]
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//
//     const child = try create_node(allocator);
//     child.set_headers(.node, 4);
//     try child.append_kv(0, 0, "key1", "val1");
//     try child.append_kv(1, 0, "key2", "val2");
//     try child.append_kv(2, 0, "key3", "val3");
//     try child.append_kv(3, 0, "key4", "val4");
//
//     const tree1: BTree = try create_node(allocator);
//     tree1.set_headers(.node, 4);
//     try tree1.append_kv(0, node_to_ptr(child), "key1", "val1");
//     try tree1.append_kv(1, 0, "key5", "val5");
//     try tree1.append_kv(2, 0, "key6", "val6");
//     try tree1.append_kv(3, 0, "key7", "val7");
//
//     //std.debug.print("initial: {s}\n", .{try tree1.to_string(allocator)});
//
//     const new_nodes = try tree1.split_child(allocator, 0);
//
//     try testing.expectEqual(3, new_nodes.len);
//     //std.debug.print("new parent: {s}\n", .{try new_nodes[0].to_string(allocator)});
//     //std.debug.print("left: {s}\n", .{try new_nodes[1].to_string(allocator)});
//     //std.debug.print("right: {s}\n", .{try new_nodes[2].to_string(allocator)});
//
//     try testing.expectEqualStrings("key1", try new_nodes[0].get_key(0));
//     try testing.expectEqualStrings("key3", try new_nodes[0].get_key(1));
//     try testing.expectEqualStrings("key5", try new_nodes[0].get_key(2));
//     try testing.expectEqualStrings("key6", try new_nodes[0].get_key(3));
//     try testing.expectEqualStrings("key7", try new_nodes[0].get_key(4));
//
//     try testing.expectEqualStrings("key1", try new_nodes[1].get_key(0));
//     try testing.expectEqualStrings("key2", try new_nodes[1].get_key(1));
//
//     try testing.expectEqualStrings("key3", try new_nodes[2].get_key(0));
//     try testing.expectEqualStrings("key4", try new_nodes[2].get_key(1));
// }

// test "find_key_position" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//
//     const child1 = try create_node(allocator);
//     child1.set_headers(.node, 2);
//     try child1.append_kv(0, 0, "key1", "val1");
//     try child1.append_kv(1, 0, "key2", "val2");
//
//     const child2 = try create_node(allocator);
//     child2.set_headers(.node, 2);
//     try child2.append_kv(0, 0, "key4", "val4");
//     try child2.append_kv(1, 0, "key5", "val5");
//
//     const tree1: BTree = try create_node(allocator);
//     tree1.set_headers(.node, 5);
//     try tree1.append_kv(0, node_to_ptr(child1), "key3", "val3");
//     try tree1.append_kv(1, node_to_ptr(child2), "key6", "val6");
//     try tree1.append_kv(2, 0, "key7", "val7");
//     try tree1.append_kv(3, 0, "key8", "val8");
//     try tree1.append_kv(4, 0, "key9", "val9");
//
//     //std.debug.print("parent: {s}\n", .{try tree1.to_string(allocator)});
//     //std.debug.print("child1: {s}\n", .{try child1.to_string(allocator)});
//     //std.debug.print("child2: {s}\n", .{try child2.to_string(allocator)});
//
//     //const pos1 = try tree1.find_key_position("key1");
//     //std.debug.print("pos1: {d}\n", .{pos1.?.idx});
//     try testing.expectEqual(0, (try tree1.find_key_position("key1")).?.idx);
//     try testing.expectEqual(1, (try tree1.find_key_position("key2")).?.idx);
//     try testing.expectEqual(0, (try tree1.find_key_position("key3")).?.idx);
//     try testing.expectEqual(0, (try tree1.find_key_position("key4")).?.idx);
//     try testing.expectEqual(1, (try tree1.find_key_position("key5")).?.idx);
// }

test "insert" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const tree1 = try create_node(allocator);

    tree1.set_headers(.leaf, 2);
    try tree1.append_kv(0, 0, "key1", "val1");
    try tree1.append_kv(1, 0, "key2", "val2");

    const tree2 = try tree1.insert(allocator, "key3", "val3");
    std.debug.print("tree2: {s}\n", .{try tree2.to_string(allocator)});

    var tree = tree2;
    var i: u64 = 0;
    while (i < 20) : (i += 1) {
        const max_len = 20;
        var buf: [max_len]u8 = undefined;
        const key = try std.fmt.bufPrint(&buf, "key{}", .{i + 4});
        const val = try std.fmt.bufPrint(&buf, "val{}", .{i + 4});

        tree = try tree.insert(allocator, key, val);
        std.debug.print(": {s}\n", .{try tree.to_string(allocator)});

        var j: u16 = 0;
        while (j < i) : (j += 1) {
            const keyj = try std.fmt.bufPrint(&buf, "key{}", .{j});
            const valj = try std.fmt.bufPrint(&buf, "val{}", .{j});

            const pos = try tree.find_key_position(keyj);

            // if (pos != null) |ppos| {

                try testing.expectEqualStrings(valj, try pos.?.node.get_key(pos.?.idx));
            // }
        }
    }
}
