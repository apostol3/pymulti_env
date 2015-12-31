import argparse
import atexit
import multiprocessing as mp
import subprocess

import pynlab

__author__ = "apostol3"


def spawn_func(command, name, i):
    subprocess.Popen("{exec} --pipe {name}_{id}".format(exec=command, name=name, id=i), stdout=subprocess.DEVNULL,
                     stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="multiplexer utility for nlab")
    parser.add_argument("-O", "--nlab-pipe", help="nlab pipe name (default: %(default)s)",
                        metavar="name", type=str, dest="nlab_name", default="nlab")
    parser.add_argument("-I", "--envs-pipe", help="enviroments pipe name (default: %(default)s)",
                        metavar="name", type=str, dest="envs_name", default="nlab_mlt")

    parser.add_argument("-e", "--existing", help="connect to exiting enviroments and do not spawn them",
                        action="store_false", dest="spawn")
    parser.add_argument("count", metavar="N", type=int, help="count of enviroments to start")
    parser.add_argument("command", metavar="exec", type=str, help="command to execute enviroments")

    args = parser.parse_args()

    pipe_str = "\\\\.\\pipe\\{}"

    lab = pynlab.NLab(pipe_str.format(args.nlab_name))

    envs = [pynlab.Env(pipe_str.format("{}_{}".format(args.envs_name, i)))
            for i in range(args.count)]

    print("connecting to nlab")
    if lab.connect():
        raise RuntimeError("nlab connection failed")
    print("connected")

    print("creating {} pipes: {}".format(args.count, ", ".join(['"{}"'.format(e.pipe.name) for e in envs])))
    [e.create() for e in envs]
    print("pipes created: {handlers}".format(handlers=", ".join([str(e.pipe.hPipe) for e in envs])))

    if args.spawn:
        print("starting {} subprocesses".format(args.count))
        subs = [mp.Process(target=spawn_func, args=(args.command, args.envs_name, i)) for i in range(args.count)]
        [s.start() for s in subs]
        print("all subs started. PID: {pids}".format(pids=", ".join([str(s.pid) for s in subs])))
        atexit.register(lambda: [s.terminate() for s in subs])

    print("waiting for connection")
    [e.wait() for e in envs]
    print("all subs connected")

    esi_n = pynlab.EStartInfo()
    esi_n.count = 0
    esi_n.mode = pynlab.SendModes.specified

    print("waiting start info from subs")
    for e in envs:
        esi = e.get_start_info()
        if not esi_n.count:
            esi_n.incount = esi.incount
            esi_n.outcount = esi.outcount
        else:
            if esi_n.incount != esi.incount or esi_n.outcount != esi.outcount:
                raise RuntimeError("Different specification received from {}".format(e.pipe.name))
        esi_n.count += esi.count
        print("get count: {}, incount: {}, outcount: {} from {}"
              .format(esi.count, esi.incount, esi.outcount, e.pipe.name))
    print("received start info from subs. count: {}, incount: {}, outcount: {}"
          .format(esi_n.count, esi_n.incount, esi_n.outcount))

    lab.set_start_info(esi_n)
    print("waiting start info from nlab")
    nsi = lab.get_start_info()
    print("received start info from nlab. count: {}".format(nsi.count))

    for e in envs:
        nsi_e = pynlab.NStartInfo()
        nsi_e.count = e.state.count
        e.set_start_info(nsi_e)
        e.lasthead = pynlab.VerificationHeader.ok

    print("working")
    while True:
        esi_n = pynlab.ESendInfo()
        esi_n.head = pynlab.VerificationHeader.ok
        for e in envs:
            if e.is_ok != pynlab.VerificationHeader.ok:
                esi_n.data.extend([None] * e.state.count)
                continue
            esi = e.get()
            if e.is_ok != pynlab.VerificationHeader.ok:
                if e.is_ok == pynlab.VerificationHeader.restart:
                    esi_n.data.extend([None] * e.state.count)
                    continue
                else:
                    print("get {} header from {}. stopping other enviroments and nlab"
                          .format(e.is_ok.name, e.pipe.name))
                    [e.stop() for e in envs if
                     e.is_ok == pynlab.VerificationHeader.ok or e.is_ok == pynlab.VerificationHeader.restart]
                    lab.stop()
                    print("stopped")
                    exit()
            esi_n.data.extend(esi.data)

        if all(e.is_ok == pynlab.VerificationHeader.restart for e in envs):
            eri_n = pynlab.ERestartInfo()
            eri_n.result = []
            [eri_n.result.extend(e.lrinfo.result) for e in envs]
            lab.restart(eri_n)
            for e in envs:
                e.lasthead = pynlab.VerificationHeader.ok
        else:
            lab.set(esi_n)

        nsi = lab.get()

        if lab.is_ok != pynlab.VerificationHeader.ok:
            if lab.is_ok == pynlab.VerificationHeader.restart:
                for e in envs:
                    nri_e = pynlab.NRestartInfo()
                    nri_e.count = e.state.count
                    e.restart(nri_e)
                continue
            else:
                print("get {} header from nlab. stopping enviroments".format(lab.is_ok.name))
                [e.stop() for e in envs]
                print("stopped")
                exit()

        nsi_e = pynlab.NSendInfo()
        nsi_e.head = pynlab.VerificationHeader.ok
        for e in envs:
            if e.lasthead != pynlab.VerificationHeader.ok:
                nsi.data = nsi.data[e.state.count:]
                continue

            nsi_e.data = nsi.data[:e.state.count]
            nsi.data = nsi.data[e.state.count:]

            e.set(nsi_e)
