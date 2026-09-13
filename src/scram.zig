//! SASL SCRAM client (RFC 5802) for SCRAM-SHA-256 and SCRAM-SHA-512.

const std = @import("std");

const b64 = std.base64.standard;

pub const ScramError = error{
    MalformedServerMessage,
    NonceMismatch,
    ServerSignatureMismatch,
    OutOfMemory,
};

pub const Sha = enum { sha256, sha512 };

fn HmacT(comptime s: Sha) type {
    return switch (s) {
        .sha256 => std.crypto.auth.hmac.sha2.HmacSha256,
        .sha512 => std.crypto.auth.hmac.sha2.HmacSha512,
    };
}

fn HashT(comptime s: Sha) type {
    return switch (s) {
        .sha256 => std.crypto.hash.sha2.Sha256,
        .sha512 => std.crypto.hash.sha2.Sha512,
    };
}

pub fn Scram(comptime s: Sha) type {
    return struct {
        const Self = @This();
        const Hmac = HmacT(s);
        const Hash = HashT(s);
        const mac_len = Hmac.mac_length;

        client_first_bare: []u8,
        server_signature: [mac_len]u8,

        /// `n,,n=<user>,r=<nonce>`; stores the bare part for the auth message.
        pub fn clientFirst(io: std.Io, alloc: std.mem.Allocator, username: []const u8) ScramError!struct { msg: []u8, state: Self } {
            var nonce_raw: [18]u8 = undefined;
            io.randomSecure(&nonce_raw) catch io.random(&nonce_raw);
            const enc = b64.Encoder;
            var nonce_buf: [enc.calcSize(18)]u8 = undefined;
            const nonce = enc.encode(&nonce_buf, &nonce_raw);

            const user_esc = try escapeUser(alloc, username);
            defer alloc.free(user_esc);

            const bare = try std.fmt.allocPrint(alloc, "n={s},r={s}", .{ user_esc, nonce });
            const msg = try std.fmt.allocPrint(alloc, "n,,{s}", .{bare});
            return .{ .msg = msg, .state = .{ .client_first_bare = bare, .server_signature = undefined } };
        }

        /// Parses server-first `r=<nonce>,s=<salt-b64>,i=<iters>` and returns the
        /// client-final message.
        pub fn serverFirst(
            self: *Self,
            alloc: std.mem.Allocator,
            server_first: []const u8,
            password: []const u8,
        ) ScramError![]u8 {
            var nonce: ?[]const u8 = null;
            var salt_b64: ?[]const u8 = null;
            var iterations: ?u32 = null;

            var it = std.mem.splitScalar(u8, server_first, ',');
            while (it.next()) |tok| {
                if (tok.len < 3 or tok[1] != '=') continue;
                switch (tok[0]) {
                    'r' => nonce = tok[2..],
                    's' => salt_b64 = tok[2..],
                    'i' => iterations = std.fmt.parseInt(u32, tok[2..], 10) catch return error.MalformedServerMessage,
                    'm' => return error.MalformedServerMessage, // mandatory extension unsupported
                    else => {},
                }
            }
            const full_nonce = nonce orelse return error.MalformedServerMessage;
            const salt_enc = salt_b64 orelse return error.MalformedServerMessage;
            const iters = iterations orelse return error.MalformedServerMessage;
            // Server nonce must extend our client nonce.
            const ridx = std.mem.indexOf(u8, self.client_first_bare, ",r=") orelse
                return error.MalformedServerMessage;
            const our_nonce_val = self.client_first_bare[ridx + 3 ..];
            if (!std.mem.startsWith(u8, full_nonce, our_nonce_val)) return error.NonceMismatch;

            var salt: [512]u8 = undefined;
            const salt_len = b64.Decoder.calcSizeForSlice(salt_enc) catch return error.MalformedServerMessage;
            if (salt_len > salt.len) return error.MalformedServerMessage;
            b64.Decoder.decode(salt[0..salt_len], salt_enc) catch return error.MalformedServerMessage;

            var salted: [mac_len]u8 = undefined;
            std.crypto.pwhash.pbkdf2(&salted, password, salt[0..salt_len], iters, Hmac) catch
                return error.MalformedServerMessage;

            var client_key: [mac_len]u8 = undefined;
            Hmac.create(&client_key, "Client Key", &salted);
            var stored: [Hash.digest_length]u8 = undefined;
            Hash.hash(&client_key, &stored, .{});

            // client-final-without-proof
            const cbind = "biws"; // base64("n,,")
            const final_no_proof = try std.fmt.allocPrint(alloc, "c={s},r={s}", .{ cbind, full_nonce });
            defer alloc.free(final_no_proof);

            const auth_message = try std.fmt.allocPrint(alloc, "{s},{s},{s}", .{
                self.client_first_bare, server_first, final_no_proof,
            });
            defer alloc.free(auth_message);

            var client_sig: [mac_len]u8 = undefined;
            Hmac.create(&client_sig, auth_message, &stored);
            var proof: [mac_len]u8 = undefined;
            for (0..mac_len) |i| proof[i] = client_key[i] ^ client_sig[i];

            var server_key: [mac_len]u8 = undefined;
            Hmac.create(&server_key, "Server Key", &salted);
            Hmac.create(&self.server_signature, auth_message, &server_key);

            const plen = b64.Encoder.calcSize(mac_len);
            var pbuf: [128]u8 = undefined;
            const proof_b64 = b64.Encoder.encode(pbuf[0..plen], &proof);
            return std.fmt.allocPrint(alloc, "{s},p={s}", .{ final_no_proof, proof_b64 }) catch
                return error.OutOfMemory;
        }

        /// Verifies `v=<server-signature-b64>` in the server-final message.
        pub fn serverFinal(self: *const Self, server_final: []const u8) ScramError!void {
            if (std.mem.startsWith(u8, server_final, "e=")) return error.MalformedServerMessage;
            if (!std.mem.startsWith(u8, server_final, "v=")) return error.MalformedServerMessage;
            const v = server_final[2..];
            var sig: [mac_len]u8 = undefined;
            const want = b64.Decoder.calcSizeForSlice(v) catch return error.MalformedServerMessage;
            if (want != mac_len) return error.MalformedServerMessage;
            b64.Decoder.decode(&sig, v) catch return error.MalformedServerMessage;
            if (!std.mem.eql(u8, &sig, &self.server_signature))
                return error.ServerSignatureMismatch;
        }
    };
}

/// SCRAM username escaping: '=' -> "=3D", ',' -> "=2C".
fn escapeUser(alloc: std.mem.Allocator, u: []const u8) ScramError![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    for (u) |ch| {
        switch (ch) {
            '=' => try out.appendSlice(alloc, "=3D"),
            ',' => try out.appendSlice(alloc, "=2C"),
            else => try out.append(alloc, ch),
        }
    }
    return out.items;
}

test "SCRAM-SHA-256 known vector (RFC 7677)" {
    // RFC 7677 example: user "user", pass "pencil",
    // client nonce r= rOprNGfwEbeRWgbNEkqO
    var gpa = std.testing.allocator;
    const S = Scram(.sha256);

    // Build state deterministically by hand rather than via clientFirst's
    // random nonce.
    var st = S{ .client_first_bare = try gpa.dupe(u8, "n=user,r=rOprNGfwEbeRWgbNEkqO"), .server_signature = undefined };
    defer gpa.free(st.client_first_bare);

    const server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    const final = try st.serverFirst(gpa, server_first, "pencil");
    defer gpa.free(final);
    try std.testing.expectEqualStrings(
        "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
        final,
    );

    try st.serverFinal("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
    try std.testing.expectError(
        error.ServerSignatureMismatch,
        st.serverFinal("v=AAAATRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="),
    );
}
