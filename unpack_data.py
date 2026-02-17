import argparse
import io
import os
import struct
import zlib


def read_u32(f):
    data = f.read(4)
    if len(data) != 4:
        raise EOFError("unexpected EOF")
    return struct.unpack("<I", data)[0]


def read_len_str(f):
    ln = read_u32(f)
    if ln == 0:
        return ""
    data = f.read(ln)
    if len(data) != ln:
        raise EOFError("unexpected EOF in string")
    return data.decode("utf-8", errors="replace")


def parse_meta(meta_comp):
    if len(meta_comp) < 4:
        raise ValueError("meta_comp too small")
    meta_uncomp_size = struct.unpack("<I", meta_comp[:4])[0]
    meta = zlib.decompress(meta_comp[4:])
    if len(meta) != meta_uncomp_size:
        print(f"[warn] meta uncompressed size mismatch: {len(meta)} != {meta_uncomp_size}")
    s = io.BytesIO(meta)
    unk0 = read_u32(s)
    unk1 = read_u32(s)
    method_name = read_len_str(s)
    ext_count = read_u32(s)
    exts = [read_len_str(s) for _ in range(ext_count)]
    group_count = read_u32(s)
    groups = [read_len_str(s) for _ in range(group_count)]
    return {"unk0": unk0, "unk1": unk1, "method_name": method_name, "exts": exts, "groups": groups}


def write_converted(out_dir, base_name, ext, data):
    conv_root = os.path.join(out_dir, "_converted", ext)
    os.makedirs(conv_root, exist_ok=True)
    out_path = os.path.join(conv_root, base_name)
    with open(out_path, "wb") as f:
        f.write(data)


def detect_embedded(data):
    magics = [
        (b"\x89PNG\r\n\x1a\n", "png"),
        (b"\xff\xd8\xff", "jpg"),
        (b"OGGS", "ogg"),
        (b"RIFF", "wav"),
        (b"DDS ", "dds"),
        (b"\x1aE\xdf\xa3", "webm"),
    ]
    for magic, ext in magics:
        idx = data.find(magic)
        if idx != -1:
            return ext, idx
    return None, None


def maybe_convert(out_dir, ext, data, name_tag):
    if ext == "sound":
        if data.startswith(b"OggS"):
            write_converted(out_dir, name_tag + ".ogg", "sound", data)
        elif data.startswith(b"RIFF"):
            write_converted(out_dir, name_tag + ".wav", "sound", data)
        return
    if ext == "texture":
        if data.startswith(b"DDS "):
            write_converted(out_dir, name_tag + ".dds", "texture", data)
        return
    if ext == "jimg_texture":
        if len(data) > 6:
            payload = data[4:]
            if payload.startswith(b"\xff\xd8\xff"):
                write_converted(out_dir, name_tag + ".jpg", "jimg_texture", payload)
        return
    if ext == "gscene":
        emb_ext, off = detect_embedded(data)
        if emb_ext is not None:
            write_converted(out_dir, name_tag + "." + emb_ext, "gscene", data[off:])
        return


def unpack(data_path, out_dir, do_convert=True):
    with open(data_path, "rb") as f:
        magic = f.read(4)
        if magic != b"RDFZ":
            raise ValueError(f"bad magic: {magic!r}")
        read_len_str(f)
        meta_comp_size = read_u32(f)
        meta_comp = f.read(meta_comp_size)
        if len(meta_comp) != meta_comp_size:
            raise EOFError("unexpected EOF in meta_comp")
        meta = parse_meta(meta_comp)
        exts = meta["exts"]
        groups = meta["groups"]
        entry_count = read_u32(f)
        entries = [struct.unpack("<5I", f.read(20)) for _ in range(entry_count)]
        name_counts = {}
        for i, (offset, size, ext_id, group_id, method_id) in enumerate(entries):
            if ext_id >= len(exts) or group_id >= len(groups):
                print(f"[warn] entry {i}: bad indices ext={ext_id} group={group_id}")
                continue
            ext = exts[ext_id]
            group = groups[group_id]
            base_name = f"{group}.{ext}"
            count = name_counts.get(base_name, 0) + 1
            name_counts[base_name] = count
            name_tag = group if count == 1 else f"{group}_{count:02d}"
            out_path = os.path.join(out_dir, ext, base_name if count == 1 else f"{name_tag}.{ext}")
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            f.seek(offset)
            blob = f.read(size)
            if len(blob) != size:
                print(f"[warn] entry {i}: short read at offset {offset}")
                continue
            if method_id == 1:
                if len(blob) < 4:
                    print(f"[warn] entry {i}: blob too small for zlib header")
                    continue
                expected_size = struct.unpack("<I", blob[:4])[0]
                try:
                    data = zlib.decompress(blob[4:])
                except zlib.error as e:
                    print(f"[warn] entry {i}: zlib error: {e}")
                    continue
                if len(data) != expected_size:
                    print(f"[warn] entry {i}: size mismatch {len(data)} != {expected_size}")
            else:
                data = blob
            with open(out_path, "wb") as out_f:
                out_f.write(data)
            if do_convert:
                maybe_convert(out_dir, ext, data, name_tag)
        print(f"Done. Wrote files to: {out_dir}")


def main():
    parser = argparse.ArgumentParser(description="Unpack Treasures of Montezuma 3 data archive")
    parser.add_argument("data", nargs="?", default="data", help="path to data archive (default: ./data)")
    parser.add_argument("-o", "--out", default="unpacked", help="output directory")
    parser.add_argument("--no-convert", action="store_true", help="disable post-conversion")
    args = parser.parse_args()
    unpack(args.data, args.out, do_convert=not args.no_convert)


if __name__ == "__main__":
    main()
