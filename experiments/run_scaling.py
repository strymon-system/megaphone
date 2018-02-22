import experlib, sys
from executor import execute
from experlib import eprint

eprint("commit: {}".format(experlib.current_commit))

def run_cmd(cmd, redirect=None, background=False):
    full_cmd = "cd /home/andreal/Src/dynamic-scaling-mechanism; {}".format(cmd)
    eprint("running: {}".format(full_cmd))
    return execute("ssh -t andreal@sgs-r820-01 \"{}\"".format(full_cmd) + 
                    (" > {}".format(redirect) if redirect else ""), async=background)

run_cmd("cargo build --release --example word_count")

def word_count_filename(experiment_name, rounds, batch, keys, open_loop, map_mode, n, w):
    return "{}/word_count_n{}_w{}_rounds{}_batch{}_keys{}_{}_{}".format(
        experlib.experdir(experiment_name), n, w, rounds, batch, keys, open_loop, map_mode)

def run_word_count(socket, rounds, batch, keys, open_loop, map_mode, n, p, w, outfile):
    return run_cmd("hwloc-bind socket:{} -- cargo run --release --example word_count -- {} {} {} {} {} -n {} -p {} -w {}".format(socket, rounds, batch, keys, open_loop, map_mode, n, p, w), outfile, True)


def word_count_open_loop_one_all_one():
    experiment_name = "word_count-open-loop-one-all-one"

    eprint("### {} ###".format(experiment_name))
    eprint(experlib.experdir(experiment_name))
    experlib.ensuredir(experiment_name)

    for batch in (1000000 + 100000 * x for x in range(5, 10)):
        for keys in (40000 * (2 ** y) for y in range(9, 11)):
            n = 2
            w = 1
            rounds=10
            # batch=1000000
            # keys=10240000
            open_loop="open-loop"
            map_mode="one-all-one"

            filename = word_count_filename(experiment_name, rounds, batch, keys, open_loop, map_mode, n, w)
            eprint("keys: {} in {}".format(keys, filename))
            experlib.waitall([run_word_count(p, rounds, batch, keys, open_loop, map_mode, n, p, w, filename) for p in range(0, 2)])


def word_count_open_loop_half_all_half_all():
    experiment_name = "word_count-open-loop-half-all-half-all"

    eprint("### {} ###".format(experiment_name))
    eprint(experlib.experdir(experiment_name))
    experlib.ensuredir(experiment_name)

    for batch in (500000 + 100000 * x for x in [5, 7, 9]):
        for keys in (40000 * (2 ** y) for y in range(10, 12)):
        # keys = 40000 * (2 ** 4)
            n = 2
            w = 4
            rounds=10
            #batch=1000000
            open_loop="open-loop"
            map_mode="half-all-half-all"

            filename = word_count_filename(experiment_name, rounds, batch, keys, open_loop, map_mode, n, w)
            eprint("keys: {} in {}".format(keys, filename))
            experlib.waitall([run_word_count(p, rounds, batch, keys, open_loop, map_mode, n, p, w, filename) for p in range(0, 2)])
