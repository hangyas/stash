const std = @import("std");
const add = @import("./root.zig").add;

pub const SuperResult = union(enum) {
    message: []const u8,
    power: u8,
};

// a nested de
pub const UserWrapper = struct {
    user: User,
    hat: []const u8,
};

pub const User = struct {
    id: u16,
    power: u16,
    name: []const u8,
};

fn name_from_ducktyping(duck: anytype) void {
    std.debug.print("duck's name: {s}\n", .{duck.name});
}

fn user_wrk() void {
    const users: [2]User = .{
        User{
            .id = 1,
            .power = 100,
            .name = "User 1",
        },
        User{
            .id = 2,
            .power = 65,
            .name = "User 2",
        },
    };

    std.debug.print("user[{d}] = {s} @ {d}\n", .{ users[0].id, users[0].name, users[0].power });
    std.debug.print("user[{d}] = {s} @ {d}\n", .{ users[1].id, users[1].name, users[1].power });

    const str1: []const u8 = "hello1";
    const str2: []const u8 = "hello2";
    if (std.mem.eql(u8, str1, str2)) {
        std.debug.print("the 2 string are eql\n", .{});
    } else {
        std.debug.print("the 2 string are NOT eql\n", .{});
    }
}

pub fn main() !void {
    // Prints to stderr (it's a shortcut based on `std.io.getStdErr()`)
    std.debug.print("All your {s} are belong to us.\n", .{"codebase"});

    std.debug.print("Can I add? 1 + 7 = {d}\n", .{add(1, 7)});

    user_wrk();

    // stdout is for the actual output of your application, for example if you
    // are implementing gzip, then only the compressed bytes should be sent to
    // stdout, not any debugging messages.
    const stdout_file = std.io.getStdOut().writer();
    var bw = std.io.bufferedWriter(stdout_file);
    const stdout = bw.writer();

    const r: SuperResult = SuperResult{ .message = "hello" };

    switch (r) {
        .message => |msg| {
            try stdout.print("it has a message: {s}", .{msg});
        },
        .power => |pwr| {
            try stdout.print("it has a power: {d}", .{pwr});
        },
    }

    try stdout.print("Run `zig build test` to run the tests.\n", .{});

    // play with mutation

    const user1: User = .{ .id = 1, .power = 2, .name = "user1" };
    // user1.name = "nename"; --- cannot assign to constant
    var user2: User = .{ .id = 1, .power = 2, .name = "user2" };

    const wrapped1: UserWrapper = .{ .user = user1, .hat = "fancy" };
    // this creates a copy of user1
    var wrapped2: UserWrapper = .{ .user = user1, .hat = "fancy" };
    const wrapped3: UserWrapper = .{ .user = user2, .hat = "fancy" };

    // updates the copy inside wrapped2
    wrapped2.user.name = "wrapped update";
    // updates only the original (wrapped3 has just a copy)
    user2.name = "user2 updated";

    // expected type '*main.User', found '*const main.User'
    //cheating(&user1);

    // this can modify however it wants because user2 is a var
    cheating(&user2);

    try stdout.print("user1: {s}\n", .{user1.name});
    try stdout.print("user2: {s}\n", .{user2.name});
    try stdout.print("wrapped1.user: {s}\n", .{wrapped1.user.name});
    try stdout.print("wrapped2.user: {s}\n", .{wrapped2.user.name});
    try stdout.print("wrapped3.user: {s}\n", .{wrapped3.user.name});

    // hasmap

	var gpa = std.heap.GeneralPurposeAllocator(.{}){};
	const allocator = gpa.allocator();

    var lookup = std.StringHashMap(User).init(allocator);

    try lookup.put("hello", user1);
    const uback = lookup.get("hello").?;
    //this would panic at runtime
    //const uback2 = lookup.get("hello2").?;

    try stdout.print("user1 lookup: {s}", .{uback.name});

    name_from_ducktyping(user1);

    try bw.flush(); // don't forget to flush!
}

fn cheating(user: *User) void {
    user.name = "cheating update";
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}
