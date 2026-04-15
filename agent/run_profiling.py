#!/usr/bin/env python3
"""Run a command under DaYu VOL+VFD HDF5 profiling and collect stat outputs."""

import argparse
import glob
import os
import shutil
import subprocess
import sys


def find_tracker_libs(explicit_vol=None, explicit_vfd=None):
    agent_dir = os.path.dirname(os.path.abspath(__file__))
    dayu_root = os.path.dirname(agent_dir)
    build_dir = os.path.join(dayu_root, "build")

    vol = explicit_vol
    vfd = explicit_vfd

    if not vol:
        candidates = glob.glob(os.path.join(build_dir, "**", "*vol*.so"), recursive=True)
        vol = candidates[0] if candidates else None
    if not vfd:
        candidates = glob.glob(os.path.join(build_dir, "**", "*vfd*.so"), recursive=True)
        vfd = candidates[0] if candidates else None

    if not vol or not vfd:
        print("ERROR: VOL/VFD .so not found. Run build.sh first.", file=sys.stderr)
        sys.exit(1)
    return os.path.abspath(vol), os.path.abspath(vfd)


def main():
    parser = argparse.ArgumentParser(description="Run a command under DaYu VOL+VFD profiling")
    parser.add_argument("--cmd", required=True, help="Command to profile (quoted)")
    parser.add_argument("--output-dir", required=True, help="Directory to collect stat outputs")
    parser.add_argument("--vol-lib", default=None, help="Path to VOL tracker .so")
    parser.add_argument("--vfd-lib", default=None, help="Path to VFD tracker .so")
    parser.add_argument("--workflow-name", default="default", help="Workflow name for organizing output")
    args = parser.parse_args()

    vol_lib, vfd_lib = find_tracker_libs(args.vol_lib, args.vfd_lib)

    env = os.environ.copy()
    env["HDF5_VOL_CONNECTOR"] = "tracker under_vol=0;under_vol_info={};path=."
    env["HDF5_DRIVER"] = "tracker"
    env["HDF5_DRIVER_CONFIG"] = "true 65536"
    env["HDF5_PLUGIN_PATH"] = os.path.dirname(vol_lib) + ":" + os.path.dirname(vfd_lib)

    print(f"VOL tracker: {vol_lib}")
    print(f"VFD tracker: {vfd_lib}")
    print(f"Command: {args.cmd}")

    result = subprocess.run(args.cmd, shell=True, env=env)

    out_dir = os.path.join(args.output_dir, args.workflow_name)
    os.makedirs(out_dir, exist_ok=True)

    collected = 0
    for pattern in ["*-vol_data_stat.json", "*-vfd_data_stat.json"]:
        for src in glob.glob(pattern) + glob.glob(os.path.join(".", "**", pattern), recursive=True):
            dst = os.path.join(out_dir, os.path.basename(src))
            if os.path.abspath(src) != os.path.abspath(dst):
                shutil.move(src, dst)
                collected += 1

    print(f"Collected {collected} stat files into {out_dir}")
    if result.returncode != 0:
        print(f"WARNING: profiled command exited with code {result.returncode}", file=sys.stderr)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
