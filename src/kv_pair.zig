const std = @import("std");

pub const kv_header_size = 4;

pub fn get_key_len(kv_pair: []u8) usize {
    const slice: *[2]u8 = kv_pair[0..2];
    return std.mem.readInt(u16, slice, .big);
}

pub fn get_key(kv_pair: []u8) []u8 {
    return kv_pair[kv_header_size .. kv_header_size + get_key_len(kv_pair)];
}

pub fn get_value_len(kv_pair: []u8) usize {
    const slice: *[2]u8 = kv_pair[2 .. 4][0..2];
    return std.mem.readInt(u16, slice, .big);
}

pub fn get_value(kv_pair: []u8) []u8 {
    const key_len = get_key_len(kv_pair);
    const val_len = get_value_len(kv_pair);
    return kv_pair[kv_header_size + key_len .. kv_header_size + key_len + val_len];
}