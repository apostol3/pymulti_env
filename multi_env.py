import argparse
import atexit
import multiprocessing as mp
import subprocess
from urllib.parse import urlparse

import pynlab

__author__ = "apostol3"


def spawn_func(command, uri):
    subprocess.Popen("{exec} --uri {uri}".format(exec=command, uri=uri).split(), stdout=subprocess.DEVNULL,
                     stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="multiplexer utility for nlab")
    parser.add_argument("-I", "--envs_uri", help="""enviroments URI in format '[tcp|winpipe]://hostname(/pipe_name|:port)'
                                                (default: %(default)s""",
                        metavar="name", type=str, dest="envs_uri", default="tcp://127.0.0.1:15005")
    parser.add_argument("-O", "--nlab_uri", help="""nlab URI in format '[tcp|winpipe]://hostname(/pipe_name|:port)'
                                                (default: %(default)s""",
                        metavar="name", type=str, dest="nlab_uri", default="tcp://127.0.0.1:5005")
    parser.add_argument("-e", "--existing", help="connect to existing environments and do not spawn them",
                        action="store_false", dest="spawn")
    parser.add_argument("count", metavar="N", type=int, help="count of environments to start")
    parser.add_argument("command", metavar="exec", type=str, help="command to execute environments")

    args = parser.parse_args()

    lab = pynlab.NLab(args.nlab_uri)

    env_uri = urlparse(args.envs_uri)
    if env_uri.scheme == 'tcp':
        uris = ['tcp://{}:{}'.format(env_uri.hostname, env_uri.port + i) for i in range(args.count)]
    elif env_uri.scheme == 'winpipe':
        uris = ['{}_{}'.format(args.envs_uri, i) for i in range(args.count)]
    else:
        raise RuntimeError('URI protocol must be tcp or winpipe')

    envs = [pynlab.Env(i) for i in uris]

    print("connecting to nlab")
    if lab.connect():
        raise RuntimeError("nlab connection failed")
    print("connected")

    print("creating {} pipes: {}".format(args.count, ", ".join(['"{}"'.format(e.uri) for e in envs])))
    [e.create() for e in envs]
    print("pipes created: {handlers}".format(handlers=", ".join([str(e.uri) for e in envs])))

    if args.spawn:
        print("starting {} subprocesses".format(args.count))

        subs = [mp.Process(target=spawn_func, args=(args.command, i)) for i in uris]
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
                raise RuntimeError("Different specification received from {}".format(e.uri))
        esi_n.count += esi.count
        print("get count: {}, incount: {}, outcount: {} from {}"
              .format(esi.count, esi.incount, esi.outcount, e.uri))
    print("received start info from subs. count: {}, incount: {}, outcount: {}"
          .format(esi_n.count, esi_n.incount, esi_n.outcount))

    lab.set_start_info(esi_n)
    print("waiting start info from nlab")
    nsi = lab.get_start_info()
    print("received start info from nlab. count: {}".format(nsi.count))

    for e in envs:
        nsi_e = pynlab.NStartInfo()
        nsi_e.count = e.state.count
        nsi_e.round_seed = lab.state.round_seed
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
                    print("get {} header from {}. stopping other environments and nlab"
                          .format(e.is_ok.name, e.uri))
                    [e.stop() for e in envs if
                     e.is_ok == pynlab.VerificationHeader.ok or e.is_ok == pynlab.VerificationHeader.restart]
                    [e.terminate() for e in envs]
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
                    nri_e.round_seed = lab.state.round_seed
                    e.restart(nri_e)
                continue
            else:
                print("get {} header from nlab. stopping environments".format(lab.is_ok.name))
                [e.stop() for e in envs]
                [e.terminate() for e in envs]
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
