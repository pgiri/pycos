# Demonstratiion of hot swap feature

# when 'client' command is given, the function of task 'client'
# ('client_proc') is replaced with 'client_proc2' function in the file
# 'hotswap_funcs.py'; similarly, if 'server' command is given,
# server's function is replaced

import sys, random
import pycos

def server_proc(task=None):
    task.set_daemon()
    # indicate that this function can be swapped
    task.hot_swappable(True)
    while True:
        try:
            n = yield task.receive()
            print('%s received: %s' % (task, n))
        except pycos.HotSwapException as exc:
            func = exc.args[0]
            print('\nnew function: %s, %s\n' % (func.__name__, func.gi_code.co_argcount))
            # replace only if new function meets certain criteria
            if func.__name__.startswith('server_proc'):
                raise
            else:
                print('\n** ignoring hot swap function %s' % (func.__name__))

def client_proc(server, i=1, task=None):
    task.set_daemon()
    task.hot_swappable(True)
    # client doesn't process HotswapException
    while True:
        print('%s: sending %s' % (task, i))
        server.send(i)
        i += 1
        yield task.sleep(random.uniform(1, 3))

def swap(func_name, file_name, task, *args, **kwargs):
    try:
        exec(open(file_name).read())
        func = locals()[func_name]
        task.hot_swap(func, *args, **kwargs)
    except:
        print('failed to load "%s" from "%s"' % (func_name, file_name))

if __name__ == '__main__':
    pycos.logger.setLevel(pycos.Logger.DEBUG)
    server = pycos.Task(server_proc)
    client = pycos.Task(client_proc, server)
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input
    while True:
        try:
            cmd = read_input().strip().lower()
            if cmd.startswith('client'):
                swap('client_proc2', 'hotswap_funcs.py', client, server)
            elif cmd.startswith('server'):
                swap('server_proc2', 'hotswap_funcs.py', server, random.choice(['log', 'sqrt']))
            elif cmd in ('quit', 'exit'):
                break
        except:
            break
