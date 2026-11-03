from mininet.net import Mininet
from mininet.node import OVSBridge
from mininet.link import TCLink
from mininet.log import setLogLevel

setLogLevel('info')

net = Mininet(
    controller=None,
    switch=OVSBridge,
    link=TCLink
)

h1 = net.addHost('h1')
h2 = net.addHost('h2')
s1 = net.addSwitch('s1')

net.addLink(h1, s1, cls=TCLink, delay='50ms')
net.addLink(h2, s1, cls=TCLink, delay='50ms')

net.start()

print("\n--- Testing connectivity ---")
print(h1.cmd('ping -c 3', h2.IP()))

net.stop()
