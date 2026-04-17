#!/usr/bin/env python3

"""
Mininet topology for demonstrating dynamic topology-change detection.
"""

import argparse

from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.topo import Topo


class OpenFlow10Switch(OVSSwitch):
  def __init__ (self, name, **params):
    params.setdefault("protocols", "OpenFlow10")
    super().__init__(name, **params)


class TopologyChangeDemo(Topo):
  def build (self):
    s1 = self.addSwitch("s1", dpid="0000000000000001")
    s2 = self.addSwitch("s2", dpid="0000000000000002")
    s3 = self.addSwitch("s3", dpid="0000000000000003")

    h1 = self.addHost("h1", ip="10.0.0.1/24")
    h2 = self.addHost("h2", ip="10.0.0.2/24")
    h3 = self.addHost("h3", ip="10.0.0.3/24")

    self.addLink(h1, s1, cls=TCLink, bw=20, delay="5ms")
    self.addLink(h2, s2, cls=TCLink, bw=20, delay="5ms")
    self.addLink(h3, s3, cls=TCLink, bw=20, delay="5ms")

    self.addLink(s1, s2, cls=TCLink, bw=20, delay="10ms")
    self.addLink(s2, s3, cls=TCLink, bw=20, delay="10ms")


def build_argument_parser ():
  parser = argparse.ArgumentParser(description="Topology change detector demo topology")
  parser.add_argument("--controller-ip", default="127.0.0.1",
                      help="Remote controller IP address")
  parser.add_argument("--controller-port", type=int, default=6633,
                      help="Remote controller TCP port")
  return parser


def run_demo (controller_ip, controller_port):
  topo = TopologyChangeDemo()
  net = Mininet(
    topo=topo,
    controller=None,
    switch=OpenFlow10Switch,
    link=TCLink,
    autoSetMacs=True,
  )

  net.addController(
    "c0",
    controller=RemoteController,
    ip=controller_ip,
    port=controller_port,
  )

  net.start()

  info("\n*** Topology Change Detector demo is running\n")
  info("*** Suggested validation sequence:\n")
  info("mininet> pingall\n")
  info("mininet> h1 ping -c 4 h3\n")
  info("mininet> iperf h1 h3\n")
  info("mininet> link s2 s3 down\n")
  info("mininet> h1 ping -c 4 h3\n")
  info("mininet> h1 ping -c 4 h2\n")
  info("mininet> link s2 s3 up\n")
  info("mininet> h1 ping -c 4 h3\n")
  info("mininet> iperf h1 h3\n\n")

  CLI(net)
  net.stop()


if __name__ == "__main__":
  setLogLevel("info")
  args = build_argument_parser().parse_args()
  run_demo(args.controller_ip, args.controller_port)
