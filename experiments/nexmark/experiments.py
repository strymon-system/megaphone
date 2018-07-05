#!/usr/bin/env python3
import sys, os
from executor import execute

is_worktree_clean = execute("cd `git rev-parse --show-toplevel`; git diff-index --quiet HEAD -- src/ Cargo.toml nexmark/src/ nexmark/Cargo.toml", check=False)

if not is_worktree_clean:
    shall = input("Work directory dirty. Continue? (y/N) ").lower() == 'y'

current_commit = ("dirty-" if not is_worktree_clean else "") + execute("git rev-parse HEAD", capture=True)
current_commit = current_commit[:16]

def eprint(*args):
    print(*args, file=sys.stderr)

def ensure_dir(name):
    if not os.path.exists(name):
        eprint("making directory: {}".format(name))
        os.makedirs(name)

def wait_all(processes):
    for p in processes:
        p.wait()

eprint("commit: {}".format(current_commit))

# assumes the experiment code is at this path on the cluster machine(s)
cluster_src_path = None
# cluster_server = "username@server"
cluster_server = None

def run_cmd(cmd, redirect=None, background=False, node="", dryrun=False):
    full_cmd = "cd {}; {}".format(cluster_src_path, cmd)
    eprint("running on {}{}: {}".format(cluster_server, node, full_cmd))
    if redirect is not None and os.path.exists(redirect):
        return execute("echo \"skipping {}\"".format(redirect), async=background)
    else:
        cmd = "ssh -t {}{} \"{}\"".format(cluster_server, node, full_cmd) + (" > {}".format(redirect) if redirect else "")
        if dryrun:
            eprint("$ {}".format(cmd))
            return execute("echo dryrun", async=background)
        else:
            return execute(cmd, async=background)
