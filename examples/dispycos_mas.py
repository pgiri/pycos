# Run 'dispycosnode.py' program on one or more nodes (to start servers to execute tasks
# sent by this client), and this program on local computer(s).

# this is a simple example of multi-agent system (https://en.wikipedia.org/wiki/Multi-agent_system)

# this generator function is sent to remote dispycos servers to run tasks (agents) there
def agent_proc(client_task, task=None):
    import random

    # register this task so server peers can discover each other
    task.register()

    # find peers (other agents) periodically; it may not be necessary to discover periodically,
    # except once at beginning, but this may be useful e.g., if UDP is lossy especially with WiFi
    agents = set()

    def peer_status(task=None):
        task.set_daemon()
        while 1:
            # broadcast discover messages to find other agents
            pycos_scheduler.discover_peers()
            status = yield task.receive(timeout=60)
            if not status:  # timed out
                continue
            if isinstance(status, pycos.PeerStatus):
                if status.status == pycos.PeerStatus.Online:
                    # get reference to task (with same name) at that location
                    agent = yield pycos.Task.locate('agent_proc', location=status.location,
                                                    timeout=3)
                    if isinstance(agent, pycos.Task):
                        pycos.logger.debug('%s: found agent at %s', task.location, agent)
                        agents.add(agent)
                else:
                    for agent in agents:
                        if agent.location == status.location:
                            pycos.logger.debug('%s: agent at %s disconnected',
                                               task.location, status.location)
                            agents.discard(agent)
                            break

    pycos_scheduler = pycos.Pycos.instance()
    # peer_status gets notifications of peers online and offline
    pycos_scheduler.peer_status(pycos.Task(peer_status))

    # in this simple case agents discover low and high valeues (silly exercise) randomly by
    # cooperating to accelerate the process
    low = high = random.uniform(1, 10000)
    while 1:
        # computation is simulated with waiting
        msg = yield task.recv(timeout=random.uniform(2, 6))
        if msg:
            pycos.logger.debug('%s received update from %s: %.3f / %.3f',
                               task.location, msg[0].location, msg[1], msg[2])
            low = msg[1]
            high = msg[2]
        else:
            n = random.uniform(1, 10000)
            if n < low:
                low = n
            elif n > high:
                high = n
            else:
                continue

            msg = (task, low, high)
            client_task.send(msg)
            # send message to agents and drop agents that may have gone away
            drop = [agent for agent in agents if agent.send(msg)]
            if drop:
                for agent in drop:
                    agents.discard(agent)


# -- code below is executed locally --

# status messages indicating nodes, servers and remote tasks finish status are sent to this local
# task; in this case we process only servers initialized and closed
def status_proc(client, task=None):
    task.set_daemon()
    while 1:
        msg = yield task.receive()
        if isinstance(msg, DispycosStatus):
            if msg.status == Scheduler.ServerInitialized:
                # start new agent at this server
                agent = yield client.rtask_at(msg.info, agent_proc, client_task)
                # there is no need to keep track of agents in this example, but done so here to
                # show potential use
                if isinstance(agent, pycos.Task):
                    agents.add(agent)
            elif msg.status == Scheduler.ServerClosed or msg.status == Scheduler.ServerAbandoned:
                for agent in agents:
                    if agent.location == msg.info:
                        agents.discard(agent)
                        break


# this local task submits client to dispycos scheduler, shows latest updates
def client_proc(task=None):
    # set status_task separately as status_proc needs 'client' argument
    client.status_task = pycos.Task(status_proc, client)
    # schedule client with the scheduler
    if (yield client.schedule()):
        raise Exception('schedule failed')

    update_from = None
    while 1:
        # as low / high values approach their limits, it may take long time to receive updates;
        # here, the value are reset to middle if no update received for 60 seconds
        msg = yield task.recv(timeout=60)
        if not msg:
            if update_from:
                pycos.logger.info('Resetting values to 5000')
                update_from.send((task, 5000, 5000))
            continue
        if msg == 'quit':
            break
        # from an agent with latest low / high values
        pycos.logger.info('Update from %s: %.3f / %.3f ', msg[0].location, msg[1], msg[2])
        update_from = msg[0]

    yield client.close(terminate=True)


if __name__ == '__main__':
    import pycos.dispycos, sys
    import pycos.netpycos as pycos
    from pycos.dispycos import *

    pycos.logger.setLevel(pycos.Logger.DEBUG)

    client = Client([agent_proc])
    agents = set()  # for illustration - not required in this example
    servers = set()
    client_task = pycos.Task(client_proc)

    print('   Enter "quit" or "exit" to end the program ')
    if sys.version_info.major > 2:
        read_input = input
    else:
        read_input = raw_input

    while True:
        try:
            inp = read_input().strip().lower()
            if inp == 'quit' or inp == 'exit':
                break
        except KeyboardInterrupt:
            break
    client_task.send('quit')
