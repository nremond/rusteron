"""Compares the snapshots services 0 and 1 took, as the recording log lists
them: the container's part of each (the frames up to the END marker) must be
the same bytes. Their own state after it is theirs, and not compared.

    python3 -I compare_snapshots.py RECORDING_LOG_DUMP ARCHIVE_DIR

RECORDING_LOG_DUMP is what `ClusterTool <dir> recording-log` printed.
Exits 1 on a difference, or when there is nothing to compare.
"""

import os
import re
import struct
import sys


def frames(archive_dir, recording_id):
    """The payloads of a recording's frames, from its segment files, in order."""
    segments = sorted(
        (int(name.split("-")[1].split(".")[0]), name)
        for name in os.listdir(archive_dir)
        if name.startswith(f"{recording_id}-") and name.endswith(".rec")
    )
    data = b"".join(open(os.path.join(archive_dir, name), "rb").read() for _, name in segments)
    payloads, offset = [], 0
    while offset + 32 <= len(data):
        (length,) = struct.unpack_from("<i", data, offset)
        if length <= 0:
            break
        frame_type = struct.unpack_from("<h", data, offset + 6)[0]
        if frame_type == 1:  # data, not padding
            payloads.append(data[offset + 32 : offset + length])
        offset += (length + 31) & ~31
    return payloads


def container_part(payloads):
    """The frames up to and including the END marker (template 100, mark 2)."""
    for i, payload in enumerate(payloads):
        template_id = struct.unpack_from("<H", payload, 2)[0] if len(payload) >= 8 else None
        if template_id == 100 and struct.unpack_from("<i", payload, 8 + 28)[0] == 2:
            return payloads[: i + 1]
    return None


def main():
    dump = open(sys.argv[1]).read()
    archive_dir = sys.argv[2]
    by_service = {}
    for entry in dump.split("Entry{")[1:]:
        if "type=SNAPSHOT" not in entry:
            continue
        recording_id = int(re.search(r"recordingId=(-?\d+)", entry).group(1))
        service_id = int(re.search(r"serviceId=(-?\d+)", entry).group(1))
        by_service.setdefault(service_id, []).append(recording_id)
    rust, java = by_service.get(0, []), by_service.get(1, [])
    if not rust or len(rust) != len(java):
        print(f"snapshots to compare: Rust {rust}, Java {java}")
        sys.exit(1)
    for rust_id, java_id in zip(rust, java):
        r = container_part(frames(archive_dir, rust_id))
        j = container_part(frames(archive_dir, java_id))
        if r is None or r != j:
            print(f"snapshot recordings {rust_id} (Rust) and {java_id} (Java) differ")
            sys.exit(1)
        print(f"snapshot recordings {rust_id} (Rust) and {java_id} (Java): {len(r)} frames alike")


main()
