#!/usr/bin/env python3
"""Generate a fleet.json for N nodes: python3 tools/gen_fleet.py 5 > fleet.json"""
import json
import sys

n = int(sys.argv[1]) if len(sys.argv) > 1 else 3
nodes = [{"name": "pg-primary", "role": "primary", "port": 5432}]
nodes += [{"name": f"pg-replica-{i}", "role": "replica", "port": 5432 + i} for i in range(1, n)]
json.dump({"cluster": "gen", "image": "postgres:16", "backup_dir": "./backups",
           "password_env": "PGFLEET_PASSWORD", "nodes": nodes}, sys.stdout, indent=2)
print()
