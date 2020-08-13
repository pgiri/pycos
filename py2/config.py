# if None, HIGHEST_PROTOCOL is used; if 0, DEFAULT_PROTOCOL is used;
# otherwise, given protocol version is used
PickleProtocolVersion = None
# timeout in seconds used when sending messages
MsgTimeout = 10
# if connections to a peer are not successful consecutively MaxConnectionErrors
# times, peer is assumed dead and removed
MaxConnectionErrors = 10

IPV4_MULTICAST_GROUP = '239.255.97.5'
IPV6_MULTICAST_GROUP = 'ff05::674f:48ba:b409:3171:9705'
NetPort = 9705

MinPulseInterval = MsgTimeout
MaxPulseInterval = 10 * MinPulseInterval
# Settings below are evaluated so must be expressions
DispycosSchedulerPort = 'pycos.config.NetPort'
DispycosNodePort = 'pycos.config.DispycosSchedulerPort + 1'
