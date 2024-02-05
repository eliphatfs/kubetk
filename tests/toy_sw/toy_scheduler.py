from kubetk.arch.scheduler import serve_scheduler, WorkQueue


def main():
    serve_scheduler(WorkQueue(list(range(100))))


if __name__ == '__main__':
    main()
