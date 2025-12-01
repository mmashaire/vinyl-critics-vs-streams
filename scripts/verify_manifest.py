import json
import sys
from pathlib import Path

def load(p: str):
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

if len(sys.argv) != 3:
    print("Usage: python scripts\\verify_manifest.py <old_manifest.json> <new_manifest.json>")
    sys.exit(2)

old = load(sys.argv[1])
new = load(sys.argv[2])

def get_tables(m): return set(m.get("tables", {}).keys())

old_tabs, new_tabs = get_tables(old), get_tables(new)
added = sorted(new_tabs - old_tabs)
removed = sorted(old_tabs - new_tabs)
common = sorted(old_tabs & new_tabs)

print(f"[info] tables (old={len(old_tabs)}, new={len(new_tabs)})")
if added:  print(f"[warn] added tables: {added}")
if removed: print(f"[warn] removed tables: {removed}")

bad = False

for t in common:
    o = old["tables"][t]
    n = new["tables"][t]
    dr = n["rows"] - o["rows"]
    pct = (dr / o["rows"] * 100) if o["rows"] else 0.0
    hash_changed = (o["sha256"] != n["sha256"])
    status = []
    if dr != 0: status.append(f"rows {o['rows']}→{n['rows']} ({pct:+.2f}%)")
    if hash_changed: status.append("hash changed")
    if status:
        print(f"[delta] {t}: " + ", ".join(status))
        # Policy example: fail if row drop >2%
        if pct < -2.0:
            bad = True

if bad:
    print("[fail] significant regressions detected")
    sys.exit(1)

print("[ok] manifest comparison passed")
