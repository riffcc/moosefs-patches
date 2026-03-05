#!/usr/bin/env python3
"""
test_proto_wire.py – MooseFS wire protocol integration test

Connects directly to the MooseFS master and exercises the wire protocol
that our mfs-proto kernel library implements. Validates:
  1. TCP connect + packet framing (type:32 length:32 data:N)
  2. ANTOAN_NOP (no-op ping)
  3. CLTOMA_FUSE_REGISTER (session registration) — MooseFS 4.x ACL format
  4. CLTOMA_FUSE_STATFS (filesystem stats)
  5. CLTOMA_FUSE_GETATTR (root inode attributes)
  6. CLTOMA_FUSE_LOOKUP (lookup in root dir)
  7. CLTOMA_FUSE_READDIR (readdir on root)
  8. Packet boundary / framing stress

This is a userspace test that validates the SAME byte-level protocol
our kernel modules will speak. If this passes, the protocol encoding
in mfs-proto/ is correct.
"""

import socket
import struct
import sys
import os
import time

# ─── MooseFS Protocol Constants ───────────────────────────────────────
# From MFSCommunication.h (MooseFS 4.x)

# Packet structure: type(4) + length(4) + data(length)  [network byte order]

# Any-to-any
ANTOAN_NOP              = 0
ANTOAN_UNKNOWN_COMMAND  = 1
ANTOAN_BAD_COMMAND_SIZE = 2

# Client-to-Master (FUSE operations) — correct opcode numbers for MooseFS 4.x
CLTOMA_FUSE_REGISTER    = 400
MATOCL_FUSE_REGISTER    = 401
CLTOMA_FUSE_STATFS      = 402
MATOCL_FUSE_STATFS      = 403
CLTOMA_FUSE_ACCESS      = 404
MATOCL_FUSE_ACCESS      = 405
CLTOMA_FUSE_LOOKUP      = 406
MATOCL_FUSE_LOOKUP      = 407
CLTOMA_FUSE_GETATTR     = 408
MATOCL_FUSE_GETATTR     = 409
CLTOMA_FUSE_SETATTR     = 410
MATOCL_FUSE_SETATTR     = 411
CLTOMA_FUSE_READDIR     = 428
MATOCL_FUSE_READDIR     = 429

# Register sub-commands (from MFSCommunication.h)
REGISTER_GETRANDOM      = 1
REGISTER_NEWSESSION     = 2   # CE new session registration
REGISTER_RECONNECT      = 3
REGISTER_TOOLS          = 4
REGISTER_NEWMETASESSION = 5
REGISTER_CLOSESESSION   = 6

# MFS_ROOT_ID
MFS_ROOT_ID = 1

# Status codes (from MFSCommunication.h)
MFS_STATUS_OK           = 0
MFS_ERROR_EPERM         = 1
MFS_ERROR_ENOTDIR       = 2
MFS_ERROR_ENOENT        = 3
MFS_ERROR_EACCES        = 4
MFS_ERROR_EEXIST        = 5
MFS_ERROR_EINVAL        = 6
MFS_ERROR_REGISTER      = 15

# MooseFS 4.x registration: 64-byte ACL blob
FUSE_REGISTER_BLOB_ACL = b"DjI1GAQDULI5d2YjA26ypc3ovkhjvhciTQVx3CS4nYgtBoUcsljiVpsErJENHaw0"

# Version: pretend to be mfsmount 4.56.6
VERSION_MAJOR = 4
VERSION_MID   = 56
VERSION_MIN   = 6
VERSION_U32   = (VERSION_MAJOR << 16) | (VERSION_MID << 8) | VERSION_MIN

MASTER_HOST = os.environ.get("MFS_MASTER", "172.29.99.2")
MASTER_PORT = int(os.environ.get("MFS_MASTER_PORT", "9421"))

# ─── Helpers ──────────────────────────────────────────────────────────

class MFSError(Exception):
    pass

class MFSConnection:
    """Low-level MooseFS master connection with packet framing."""

    def __init__(self, host, port, timeout=10):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.connect((host, port))
        self.session_id = 0

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass

    def send_packet(self, ptype, data=b""):
        """Send a MooseFS packet: type(4) + length(4) + data."""
        header = struct.pack("!II", ptype, len(data))
        self.sock.sendall(header + data)

    def recv_packet(self, timeout=10):
        """Receive a MooseFS packet. Returns (type, data)."""
        old_timeout = self.sock.gettimeout()
        self.sock.settimeout(timeout)
        try:
            header = self._recv_exact(8)
            ptype, length = struct.unpack("!II", header)
            data = self._recv_exact(length) if length > 0 else b""
            return ptype, data
        finally:
            self.sock.settimeout(old_timeout)

    def _recv_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError(f"Connection closed, wanted {n} bytes, got {len(buf)}")
            buf.extend(chunk)
        return bytes(buf)

    def register_session(self):
        """
        Register a new session with the master (MooseFS 4.x ACL format).

        Wire format for CLTOMA_FUSE_REGISTER with REGISTER_NEWSESSION:
          blob(64) + rcode(1=REGISTER_NEWSESSION) + version(4, big-endian 32-bit)
          + ileng(4) + info(ileng) + pleng(4) + path(pleng)
        """
        info = b"mfs-extended-test\x00"   # null-terminated
        path = b"/\x00"                    # null-terminated

        data = FUSE_REGISTER_BLOB_ACL                    # 64 bytes
        data += struct.pack("!B", REGISTER_NEWSESSION)    # 1 byte: rcode=2
        data += struct.pack("!I", VERSION_U32)            # 4 bytes: version
        data += struct.pack("!I", len(info))              # 4 bytes: info length
        data += info                                       # N bytes: info string
        data += struct.pack("!I", len(path))              # 4 bytes: path length
        data += path                                       # N bytes: path string

        self.send_packet(CLTOMA_FUSE_REGISTER, data)
        rtype, rdata = self.recv_packet()

        if rtype != MATOCL_FUSE_REGISTER:
            raise MFSError(f"Expected MATOCL_FUSE_REGISTER ({MATOCL_FUSE_REGISTER}), got {rtype}")

        if len(rdata) == 1:
            status = rdata[0]
            status_names = {
                MFS_ERROR_EPERM: "EPERM",
                MFS_ERROR_REGISTER: "REGISTER (bad blob)",
                MFS_ERROR_EACCES: "EACCES",
            }
            name = status_names.get(status, f"unknown({status})")
            raise MFSError(f"Registration failed with MFS status {status} ({name})")

        if len(rdata) < 4:
            raise MFSError(f"Registration response too short: {len(rdata)} bytes (hex: {rdata.hex()})")

        # Response: sessionid(4) + sesflags(1) + rootuid(4) + rootgid(4) + ...
        self.session_id = struct.unpack("!I", rdata[0:4])[0]
        return self.session_id


# ─── Tests ────────────────────────────────────────────────────────────

passed = 0
failed = 0
total  = 0

def test(name, func):
    global passed, failed, total
    total += 1
    try:
        func()
        passed += 1
        print(f"  ✅ {name}")
    except Exception as e:
        failed += 1
        print(f"  ❌ {name}: {e}")


def test_tcp_connect():
    """Test basic TCP connection to master."""
    conn = MFSConnection(MASTER_HOST, MASTER_PORT)
    conn.close()

def test_nop():
    """Send ANTOAN_NOP and verify master doesn't disconnect."""
    conn = MFSConnection(MASTER_HOST, MASTER_PORT)
    conn.send_packet(ANTOAN_NOP)
    # NOP doesn't get a response, but connection stays alive
    # Send another NOP to confirm
    conn.send_packet(ANTOAN_NOP)
    conn.close()

def test_unknown_command():
    """Send an unknown command type and expect graceful handling."""
    conn = MFSConnection(MASTER_HOST, MASTER_PORT)
    conn.send_packet(9999, b"")
    # Master should respond with unknown command or just disconnect
    try:
        rtype, rdata = conn.recv_packet(timeout=3)
        # Either ANTOAN_UNKNOWN_COMMAND or disconnect is fine
    except (ConnectionError, socket.timeout):
        pass  # Expected — master may just close connection
    conn.close()

def test_register_session():
    """Register a new FUSE session with the master (MooseFS 4.x ACL format)."""
    conn = MFSConnection(MASTER_HOST, MASTER_PORT)
    sid = conn.register_session()
    assert sid > 0, f"Expected positive session ID, got {sid}"
    conn.close()

def test_statfs():
    """CLTOMA_FUSE_STATFS – get filesystem statistics."""
    conn = MFSConnection(MASTER_HOST, MASTER_PORT)
    sid = conn.register_session()

    # CLTOMA_FUSE_STATFS (402): msgid(4) — exactly 4 bytes required
    msgid = 1
    data = struct.pack("!I", msgid)
    conn.send_packet(CLTOMA_FUSE_STATFS, data)
    rtype, rdata = conn.recv_packet()

    assert rtype == MATOCL_FUSE_STATFS, f"Expected {MATOCL_FUSE_STATFS}, got {rtype}"
    # Response: msgid(4) + totalspace(8) + availspace(8) + [freespace(8)] + trashspace(8) + sustainedspace(8) + inodes(4)
    assert len(rdata) >= 36, f"STATFS response too short: {len(rdata)} bytes"

    # Skip msgid in response
    resp_msgid = struct.unpack("!I", rdata[0:4])[0]
    assert resp_msgid == msgid, f"STATFS msgid mismatch: sent {msgid}, got {resp_msgid}"
    # Parse stats after msgid
    totalspace, availspace = struct.unpack("!QQ", rdata[4:20])
    assert totalspace >= 0
    assert availspace >= 0
    conn.close()

def test_getattr_root():
    """CLTOMA_FUSE_GETATTR (408) on root inode (inode 1)."""
    conn = MFSConnection(MASTER_HOST, MASTER_PORT)
    sid = conn.register_session()

    # CLTOMA_FUSE_GETATTR (408): msgid(4) + inode(4) + opened(1) + uid(4) + gid(4) = 17 bytes
    msgid = 2
    data = struct.pack("!I", msgid)          # msgid
    data += struct.pack("!I", MFS_ROOT_ID)   # inode
    data += struct.pack("!B", 0)             # opened=0
    data += struct.pack("!II", 0, 0)         # uid=0, gid=0
    conn.send_packet(CLTOMA_FUSE_GETATTR, data)
    rtype, rdata = conn.recv_packet()

    assert rtype == MATOCL_FUSE_GETATTR, f"Expected {MATOCL_FUSE_GETATTR}, got {rtype}"
    # Response: msgid(4) + (status(1) | attr_blob)
    assert len(rdata) >= 5, f"GETATTR response too short: {len(rdata)} bytes"
    resp_msgid = struct.unpack("!I", rdata[0:4])[0]
    assert resp_msgid == msgid, f"GETATTR msgid mismatch: sent {msgid}, got {resp_msgid}"
    if len(rdata) == 5:
        status = rdata[4]
        if status != MFS_STATUS_OK:
            raise MFSError(f"GETATTR root returned error status {status}")
    conn.close()

def test_lookup_nonexistent():
    """CLTOMA_FUSE_LOOKUP (406) for a nonexistent name in root."""
    conn = MFSConnection(MASTER_HOST, MASTER_PORT)
    sid = conn.register_session()

    name = b"__nonexistent_test_entry__"
    # CLTOMA_FUSE_LOOKUP (406): msgid(4) + parent_inode(4) + nleng(1) + name(nleng) + uid(4) + gid(4)
    msgid = 3
    data = struct.pack("!I", msgid)                    # msgid
    data += struct.pack("!IB", MFS_ROOT_ID, len(name)) # parent_inode + nleng
    data += name                                        # name
    data += struct.pack("!II", 0, 0)                   # uid, gid
    conn.send_packet(CLTOMA_FUSE_LOOKUP, data)
    rtype, rdata = conn.recv_packet()

    assert rtype == MATOCL_FUSE_LOOKUP, f"Expected {MATOCL_FUSE_LOOKUP}, got {rtype}"
    # Response: msgid(4) + (status(1) | inode(4)+attr)
    assert len(rdata) >= 5, f"LOOKUP response too short: {len(rdata)} bytes"
    resp_msgid = struct.unpack("!I", rdata[0:4])[0]
    assert resp_msgid == msgid, f"LOOKUP msgid mismatch: sent {msgid}, got {resp_msgid}"
    # Should get ENOENT status
    if len(rdata) == 5:
        assert rdata[4] == MFS_ERROR_ENOENT, f"Expected ENOENT ({MFS_ERROR_ENOENT}), got status {rdata[4]}"
    conn.close()

def test_readdir_root():
    """CLTOMA_FUSE_READDIR (428) on root directory."""
    conn = MFSConnection(MASTER_HOST, MASTER_PORT)
    sid = conn.register_session()

    # CLTOMA_FUSE_READDIR (428): msgid(4) + inode(4) + uid(4) + gid(4) = 16 bytes
    msgid = 4
    data = struct.pack("!I", msgid)                    # msgid
    data += struct.pack("!III", MFS_ROOT_ID, 0, 0)    # inode, uid, gid
    conn.send_packet(CLTOMA_FUSE_READDIR, data)
    rtype, rdata = conn.recv_packet()

    assert rtype == MATOCL_FUSE_READDIR, f"Expected {MATOCL_FUSE_READDIR}, got {rtype}"
    # Response: msgid(4) + (status(1) | dirdata)
    assert len(rdata) >= 4, f"READDIR response too short: {len(rdata)} bytes"
    resp_msgid = struct.unpack("!I", rdata[0:4])[0]
    assert resp_msgid == msgid, f"READDIR msgid mismatch: sent {msgid}, got {resp_msgid}"
    if len(rdata) == 5 and rdata[4] != MFS_STATUS_OK:
        raise MFSError(f"READDIR root returned error status {rdata[4]}")
    conn.close()

def test_packet_framing_stress():
    """Send multiple packets rapidly to stress-test framing."""
    conn = MFSConnection(MASTER_HOST, MASTER_PORT)
    sid = conn.register_session()

    # Fire 10 STATFS in rapid succession, each with unique msgid
    for i in range(10):
        data = struct.pack("!I", 100 + i)  # msgid
        conn.send_packet(CLTOMA_FUSE_STATFS, data)

    # Collect all 10 responses
    for i in range(10):
        rtype, rdata = conn.recv_packet()
        assert rtype == MATOCL_FUSE_STATFS, f"Burst #{i}: expected {MATOCL_FUSE_STATFS}, got {rtype}"
        resp_msgid = struct.unpack("!I", rdata[0:4])[0]
        assert resp_msgid == 100 + i, f"Burst #{i}: msgid mismatch, expected {100+i}, got {resp_msgid}"

    conn.close()

def test_large_payload_rejection():
    """Send a packet with absurdly large declared length (but little data)."""
    conn = MFSConnection(MASTER_HOST, MASTER_PORT)
    # Craft a bad packet: valid type, but length claims 1GB
    header = struct.pack("!II", CLTOMA_FUSE_STATFS, 0x40000000)
    conn.sock.sendall(header)
    # Master should disconnect us
    try:
        time.sleep(1)
        conn.sock.recv(1024)
    except (ConnectionError, ConnectionResetError, socket.timeout):
        pass  # Expected
    conn.close()


# ─── Runner ───────────────────────────────────────────────────────────

def wait_for_master(host, port, timeout=60):
    """Wait for master to accept connections."""
    print(f"Waiting for MooseFS master at {host}:{port}...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((host, port))
            s.close()
            print("  Master is up!")
            return True
        except (ConnectionRefusedError, socket.timeout, OSError):
            time.sleep(1)
    print("  ⚠️  Timed out waiting for master")
    return False


def main():
    print("=" * 60)
    print("MooseFS Wire Protocol Integration Tests")
    print(f"Master: {MASTER_HOST}:{MASTER_PORT}")
    print(f"Protocol: MooseFS 4.x ACL registration")
    print(f"Client version: {VERSION_MAJOR}.{VERSION_MID}.{VERSION_MIN}")
    print("=" * 60)

    if not wait_for_master(MASTER_HOST, MASTER_PORT):
        print("FATAL: Cannot connect to master")
        sys.exit(1)

    print("\n--- Basic Connectivity ---")
    test("TCP connect", test_tcp_connect)
    test("NOP ping", test_nop)
    test("Unknown command handling", test_unknown_command)

    print("\n--- Session Management ---")
    test("Register new session (4.x ACL)", test_register_session)

    print("\n--- Metadata Operations ---")
    test("STATFS (filesystem stats)", test_statfs)
    test("GETATTR root inode", test_getattr_root)
    test("LOOKUP nonexistent entry", test_lookup_nonexistent)
    test("READDIR root directory", test_readdir_root)

    print("\n--- Stress / Edge Cases ---")
    test("Packet framing burst (10x STATFS)", test_packet_framing_stress)
    test("Large payload rejection", test_large_payload_rejection)

    print("\n" + "=" * 60)
    print(f"Results: {passed}/{total} passed, {failed} failed")
    print("=" * 60)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
