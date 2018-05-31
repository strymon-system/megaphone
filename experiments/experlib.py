import sys
from executor import execute

is_worktree_clean= execute("git diff-index --quiet HEAD --", check=False)
current_commit = ("dirty" if not is_worktree_clean else "") + execute("git rev-parse HEAD", capture=True)

def eprint(*args):
    print(*args, file=sys.stderr)

def experdir(name):
    return "results/{}/{}".format(current_commit, name)

def ensuredir(name):
    eprint("making directory: {}".format(experdir(name)))
    execute("mkdir -p {}".format(experdir(name)))

def waitall(processes):
    for p in processes:
        p.wait(use_spinner=False)

