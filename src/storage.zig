const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

// const DBError = @import("./database.zig").DBError;
// const BTree = @import("./database.zig").BTree;
const NodeBuilder = @import("./node_builder.zig").NodeBuilder;
const Node = @import("./node.zig").Node;

const StorageImplType = enum {
    in_memory,
    // on_disk
};

pub const Storage = union(StorageImplType) {
    in_memory: InMemoryStorageImpl,
    // on_disk: OnDiskStorageImpl

    pub fn createInMemory(allocator: Allocator, node_size: usize) Storage {
        return Storage{ .in_memory = InMemoryStorageImpl{ .allocator = allocator, .node_size = node_size } };
    }

    pub fn destroy() void {
        // TODO deallocate storage
    }

    pub fn node_from_ptr(self: Storage, ptr_int: u64) Node {
        return switch (self) {
            .in_memory => self.in_memory.node_from_ptr(ptr_int),
        };
    }

    pub fn node_to_ptr(self: Storage, node: Node) u64 {
        return switch (self) {
            .in_memory => self.in_memory.node_to_ptr(node),
        };
    }
};

pub const InMemoryStorageImpl = struct {
    node_size: usize,
    allocator: Allocator,

    pub fn create(allocator: Allocator) Storage {
        return Storage{ .in_memory = InMemoryStorageImpl{ .allocator = allocator, .node_size = 5 } };
    }

    pub fn node_from_ptr(self: InMemoryStorageImpl, ptr_int: u64) Node {
        const data_ptr: [*]u8 = @ptrFromInt(ptr_int);
        const data_slice: []u8 = data_ptr[0..self.node_size];
        return Node{ .data = data_slice };
    }

    pub fn node_to_ptr(_: InMemoryStorageImpl, node: Node) u64 {
        return @intFromPtr(node.data.ptr);
    }
};

test "basic add functionality" {
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const allocator = gpa.allocator();

    // try testing.expect(3 + 7 == 10);

    // const storage = Storage{ .in_memory = InMemoryStorageImpl{ .allocator = allocator, .node_size = 5 } };

    // try testing.expect(10 == storage.new_node());

    // _ = storage.createNode();
}
