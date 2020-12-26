# use with 'hotswap.py'

# 'server_proc2' replaces 'server' task's function
def server_proc2(fname, task=None):
    import math
    func = getattr(math, fname)
    task.set_daemon()
    task.hot_swappable(True)
    while True:
        try:
            n = yield task.receive()
            print('  * %s received: %s,  %s=%.2f' % (task, n, fname, func(n)))
        except pycos.HotSwapException as exc:
            gen = exc.args[0]
            if gen.__name__.startswith('server_proc'):
                print('\nreplacing with function: %s\n' % (gen.__name__))
                raise
            else:
                print('\n** ignoring hot swap function %s' % (gen.__name__))


# 'client_proc2' replaces 'client' task's function
def client_proc2(server, i=42, task=None):
    task.set_daemon()
    task.hot_swappable(True)
    while True:
        print('  * %s: sending %s' % (task, i))
        server.send(i)
        i += 2
        yield task.sleep(random.uniform(2, 6))
