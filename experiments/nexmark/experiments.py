#!/usr/bin/env python3
import sys, os, datetime
from executor import execute
import time
import shlex

is_worktree_clean = execute("cd `git rev-parse --show-toplevel`; git diff-index --quiet HEAD -- src/ Cargo.toml nexmark/src/ nexmark/Cargo.toml", check=False)

if not is_worktree_clean:
    shall = input("Work directory dirty. Continue? (y/N) ").lower() == 'y'
    if not shall:
        sys.exit(1)

current_commit = ("dirty-" if not is_worktree_clean else "") + execute("git rev-parse HEAD", capture=True)
current_commit = current_commit[:16]

def eprint(*args, level=None):
    attr = []
    if level == "info":
        attr.append('1')
        attr.append('37')
        attr.append('4')
    elif level == "run":
        attr.append('32')
    print("[" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] ", '\x1b[%sm' % ';'.join(attr), *args, '\x1b[0m', file=sys.stderr, sep='')

def ensure_dir(name):
    if not os.path.exists(name):
        os.makedirs(name)

def wait_all(processes):
    for p in processes:
        # p.wait(use_spinner=False)
        while p.is_running:
            time.sleep(.1)
        p.check_errors()

eprint("commit: {}".format(current_commit))

# assumes the experiment code is at this path on the cluster machine(s)
cluster_src_path = None
# cluster_server = "username@server"
cluster_server = None

def run_cmd(cmd, redirect=None, stderr=False, background=False, node="", dryrun=False):
    full_cmd = "cd {}; {}".format(cluster_src_path, cmd)
    # eprint("running on {}{}: {}".format(cluster_server, node, full_cmd))
    # if redirect is not None and os.path.exists(redirect):
    #     return execute("echo \"skipping {}\"".format(redirect), async=background)
    cmd = "ssh -o StrictHostKeyChecking=no -T {}{}.ethz.ch {}".format(cluster_server, node, shlex.quote(full_cmd))\
          + (" > {}".format(shlex.quote(redirect)) if redirect else "")\
          + (" 2> {}".format(shlex.quote(stderr)) if stderr else "")
    eprint("$ {}".format(cmd), level="run")
    if dryrun:
        return execute("echo dryrun {}".format(node), async=background)
    else:
        return execute(cmd, async=background)
