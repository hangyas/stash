const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

const Node = @import("./node.zig").Node;
const Position = @import("./node.zig").Position;
const NodeBuilder = @import("./node_builder.zig").NodeBuilder;
const kv_header_size = @import("./node.zig").kv_header_size;
const NodeKind = @import("./node.zig").NodeKind;
const Storage = @import("./storage.zig").Storage;
const createInMemoryStorage = @import("./storage.zig").createInMemoryStorage;

pub const Tree = struct {
    root: Node,

    allocator: Allocator,
    storage: Storage,

    pub fn init(allocator: Allocator, storage: Storage) Tree {
        const builder = NodeBuilder.init(storage, allocator, null);
        builder.setHeaders(.leaf, 0);
        const root = builder.build();

        return .{
            .allocator = allocator,
            .root = root,
            .storage = storage
        };
    }

    //
    // READ
    //

    fn get(self: Tree, key: []const u8) ?[]const u8 {
        if (self.find_key_position_recursively(self.root, key)) |pos| {
            return pos.node.get_value(pos.idx);
        } else {
            return null;
        }
    }

    // todo tail optimization?
    fn find_key_position_recursively(self: Tree, node: Node, key: []const u8) ?Position {
        var i: u16 = 0;
        while (i < node.key_count() and std.mem.order(u8, node.get_key(i), key) == .lt) {
            i += 1;
        }

        if (i < node.key_count() and std.mem.order(u8, key, node.get_key(i)) == .eq) {
            return Position{ .node = node, .idx = i };
        }

        // TODO 0 pointers shouldn't be there or handled properly
        if (node.kind() == .node and node.get_ptr(i) != 0) {
            return self.find_key_position_recursively(
                self.storage.node_from_ptr(node.get_ptr(i)),
                key
            );
        }

        return null;
    }

    //
    // WRITE
    //

    fn put(self: *Tree, key: []const u8, value: []const u8) void {
        // create a new root based on the current root
        // this.root = builder.build()
        //
        // this struct is mutable, but the nodes aren't

        var builder = NodeBuilder.init(self.storage, self.allocator, self.root);
        builder = insertRecursively(&builder, key, value);
        self.root = builder.build();
    }
};

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
//ptr[i] is elements < keys[i]
// it's possible for the parent to have 0 keys and 1 child before split
fn splitChild(builder: *NodeBuilder, idx: u16) struct{ updated_node: NodeBuilder, left: NodeBuilder, right: NodeBuilder } {

    const storage = builder.storage;

    // TODO this should be in place

    // note: first key is duplicate from parent
    const child_ptr = builder.root.get_ptr(idx);
    const child: Node = storage.node_from_ptr(child_ptr);
    const pivot_idx = child.key_count() / 2;

    // todo is the rounding of size ok like this?
    // 1 goes up to the parent, others are split
    const left_child_size = pivot_idx;
    // pivot element is moved to the parents -> +/- 1
    const right_child_size = child.key_count() - pivot_idx - 1;
    // const parent_size = builder.root.key_count() + 1;

    // todo link this 2 builders under the builder
    const left = builder.createChildBuilder();
    const right = builder.createChildBuilder();
    // const updated_node = storage.createNodeBuilder();

    // creating and initializing the new child
    left.setHeaders(child.kind(), left_child_size);
    right.setHeaders(child.kind(), right_child_size);
    // updated_node.set_headers(node.kind(), parent_size);

    // left
    left.copyRange(child, 0, 0, left_child_size);
    // copy the last right pointer too
    left.copyLeftPtrs(child, 0, 0, left_child_size + 1);

    // right
    right.copyRange(child, 0, pivot_idx + 1, right_child_size);
    // copy the last right pointer too
    left.copyLeftPtrs(child, 0, pivot_idx + 1, right_child_size + 1);

    // parent
    // updated_node.copyRange(node, 0, 0, idx);    -- it's already there

    // todo remove ptr from append_kv and set them separately (?)
    builder.shiftAndInsert(idx, child.get_key(pivot_idx), child.get_value(pivot_idx));
    // if (builder.node.key_count() - idx > 0) {
    //     updated_node.node_copy_range(node, idx + 1, idx, node.key_count() - idx - 1);
    // }
    // parent pointers
    // builder.node_copy_left_ptrs(node, 0, 0, idx);
    // updated_node.set_ptr(idx, storage.node_to_ptr(left));
    // updated_node.set_ptr(idx + 1, storage.node_to_ptr(right));
    // if (node.key_count() - idx > 2) {
    //     updated_node.node_copy_right_ptrs(node, idx + 2, idx + 1, node.key_count() - idx - 2);
    // }
    //
    return .{ .updated_node = builder.*, .left = left, .right = right };
}

fn insertRecursively(builder: *NodeBuilder, key: []const u8, value: []const u8) NodeBuilder {
    // inserts into the builder, returns the new root builder
    // (root can change when is full and needs to be split)

    if (builder.isFull()) {
        std.debug.print("insert {s} into full\n", .{key});

        // need an empty root with 1 child that can be split
        // has 0 keys, but 1 child

        // todo it's a parent builder tho :/
        var newRoot = builder.createChildBuilder();
        newRoot.setHeaders(.node, 0);
        newRoot.setPtr(0, builder.storage.node_to_ptr(builder.root));

        _ = splitChild(&newRoot, 0);

        insertIntoNonFullTree(&newRoot, key, value);
        return newRoot;
    } else {
        std.debug.print("insert {s} into non-full\n", .{key});

        // just use the current node
        insertIntoNonFullTree(builder, key, value);
        return builder.*;
    }

}

fn insertIntoNonFullTree(builder: *NodeBuilder, key: []const u8, value: []const u8) void {
    if (builder.root.kind() == .leaf) {
        // look up position and insert
        var i: u16 = 0;

        while (i < builder.root.key_count() and std.mem.order(u8, builder.root.get_key(i), key) == .lt) {
            i += 1;
        }

        std.debug.print("shift and insert at {d}\n", .{i});
        builder.shiftAndInsert(i, key, value);
    } else {
        var i: u16 = 0;
        while (i < builder.root.key_count() and std.mem.order(u8, builder.root.get_key(i), key) == .lt) {
            i += 1;
        }

        // it's an internal node so pointers aren't empty
        var target: Node = builder.storage.node_from_ptr(builder.root.get_ptr(i));

        // todo move constant
        const t = 4;

        // if it's full then split it
        if (target.key_count() > 2 * t - 1) {
            const updated_nodes = splitChild(builder, i);

            // check if it went to left or right
            if (std.mem.order(u8, updated_nodes.updated_node.root.get_key(i), key) == .lt) {
                i += 1;
            }
            target = builder.storage.node_from_ptr(updated_nodes.updated_node.root.get_ptr(i));
        }

        var targetBuilder = builder.createChildBuilder();
        targetBuilder.copy(target);

        // todo should recuresively Tree.insert (it doesn't need to be a tree then)
        const newRoot = insertRecursively(&targetBuilder, key, value);

        builder.setPtr(i, builder.storage.node_to_ptr(newRoot.root));
    }
}



test "leaf_insert" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    const str = Storage.createInMemory(allocator, 512);

    var tree = Tree.init(allocator, str);

    tree.put("key", "value");
    tree.put("next-key", "value");
    // tree.put("long-key", "longer-value");

    try testing.expectEqualStrings("value", tree.get("key").?);
    // try testing.expectEqualStrings("val-longer", try tree2.get_value(0));
    // try testing.expectEqualStrings("inserted-key", try tree2.get_key(1));
    // try testing.expectEqualStrings("inserted-val", try tree2.get_value(1));
    // try testing.expectEqualStrings("next-key", try tree2.get_key(2));
    // try testing.expectEqualStrings("val-other", try tree2.get_value(2));
}

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

// test "insert" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     const allocator = gpa.allocator();
//
//     const tree1 = try create_node(allocator);
//
//     tree1.set_headers(.leaf, 2);
//     try tree1.append_kv(0, 0, "key1", "val1");
//     try tree1.append_kv(1, 0, "key2", "val2");
//
//     const tree2 = try tree1.insert(allocator, "key3", "val3");
//     std.debug.print("tree2: {s}\n", .{try tree2.to_string(allocator)});
//
//     var tree = tree2;
//     var i: u64 = 0;
//     while (i < 20) : (i += 1) {
//         const max_len = 20;
//         var buf: [max_len]u8 = undefined;
//         const key = try std.fmt.bufPrint(&buf, "key{}", .{i + 4});
//         const val = try std.fmt.bufPrint(&buf, "val{}", .{i + 4});
//
//         tree = try tree.insert(allocator, key, val);
//         std.debug.print(": {s}\n", .{try tree.to_string(allocator)});
//
//         var j: u16 = 0;
//         while (j < i) : (j += 1) {
//             const keyj = try std.fmt.bufPrint(&buf, "key{}", .{j});
//             const valj = try std.fmt.bufPrint(&buf, "val{}", .{j});
//
//             const pos = try tree.find_key_position(keyj);
//
//             // if (pos != null) |ppos| {
//
//             try testing.expectEqualStrings(valj, try pos.?.node.get_key(pos.?.idx));
//             // }
//         }
//     }