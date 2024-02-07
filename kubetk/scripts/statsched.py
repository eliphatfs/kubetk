import time
import datetime
import textwrap
import argparse
import threading
import subprocess
import kubetk.arch.worker as wk


def clear():
    print("\033c", end="")


def print_stats(rpc, cls=False):
    stats = rpc.stat()
    if cls:
        clear()
    print("Remaining:", stats['remain'])
    print("On-going:", stats['ongoing'])
    print("Total Success:", stats['success'])
    print("Success after Back-off:", stats['backoff_success'])
    print("Failure:", stats['backoff_failure'])
    if stats['throughput'] >= 0.5:
        print("Throughput:", stats['throughput'], '/', 'sec')
    else:
        print("Throughput:", stats['throughput'] * 60, '/', 'min')
    print("ETA:", datetime.timedelta(0, stats['eta']))


def print_messages(rpc):
    flag = False
    for msg in rpc.list_messages():
        flag = True
        print(msg)
    if not flag:
        print("<no error>")


def dup(x):
    for line in x:
        print(line.decode(), end='')


def main():
    argp = argparse.ArgumentParser(description=textwrap.dedent("""
        Get statistics from scheduler.
    """))
    argp.add_argument("command", choices=['watch', 'stat', 'errors'])
    argp.add_argument("--pod", default=None, help="instead of an IP address, specify the scheduler as a pod")
    argp.add_argument("--ip", default=None, help="IP of scheduler")
    argp.add_argument("--port", default=9105, help="Port of scheduler")
    argp.add_argument("--https", action='store_true', help="use HTTPS secure transport")
    args = argp.parse_args()
    assert int(args.pod is not None) + int(args.ip is not None) == 1
    if args.ip is None:
        proc = subprocess.Popen(
            ["kubectl", "port-forward", f"{args.pod}", f"{args.port}:{args.port}"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        args.ip = '127.0.0.1'
        print(proc.stdout.readline().decode(), end='')
        threading.Thread(target=dup, args=[proc.stdout], daemon=True).start()
    else:
        proc = None
    try:
        rpc = wk.get_rpc_object(('https' if args.https else 'http') + "://" + args.ip + ":" + str(args.port))
        while args.command == 'watch':
            time.sleep(2)
            print_stats(rpc, True)
        if args.command == 'stat':
            print_stats(rpc)
        if args.command == 'errors':
            print_messages(rpc)
    finally:
        if proc is not None:
            proc.terminate()


if __name__ == "__main__":
    main()
