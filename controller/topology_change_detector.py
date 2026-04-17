"""
POX controller for dynamic topology-change detection and reactive L2 forwarding.
"""

from collections import defaultdict
import json
from pathlib import Path
import time

from pox.core import core
from pox.lib.util import dpid_to_str
import pox.openflow.libopenflow_01 as of

log = None


class TopologyChangeDetector(object):
  """
  Detect topology changes and install reactive forwarding rules.
  """

  def __init__ (self, flow_idle_timeout = 20, flow_hard_timeout = 60,
                log_file = None, state_file = None):
    self.flow_idle_timeout = int(flow_idle_timeout)
    self.flow_hard_timeout = int(flow_hard_timeout)

    self.connections = {}
    self.switches = {}
    self.hosts = {}
    self.directed_links = set()
    self.mac_to_port = defaultdict(dict)

    self.repo_root = Path(__file__).resolve().parents[1]
    self.log_file = self._resolve_output_path(log_file, "topology_events.log")
    self.state_file = self._resolve_output_path(state_file, "topology_state.json")

    core.listen_to_dependencies(
      self,
      components=["openflow", "openflow_discovery"],
      listen_args={
        "openflow": {"priority": 0},
        "openflow_discovery": {"priority": 0},
      },
    )

  def _all_dependencies_met (self):
    self._write_state()
    self._log_event(
      "controller_start",
      "Topology Change Detector is ready",
      flow_idle_timeout=self.flow_idle_timeout,
      flow_hard_timeout=self.flow_hard_timeout,
    )

  def _resolve_output_path (self, output_path, default_name):
    artifacts_dir = self.repo_root / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    if output_path is None:
      path = artifacts_dir / default_name
    else:
      path = Path(output_path).expanduser()
      if not path.is_absolute():
        path = self.repo_root / path

    path.parent.mkdir(parents=True, exist_ok=True)
    return path

  def _timestamp (self):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

  def _log_event (self, event_type, message, **fields):
    field_text = " ".join("%s=%s" % (key, value) for key, value in sorted(fields.items()))
    line = "%s | %s | %s" % (self._timestamp(), event_type, message)
    if field_text:
      line = "%s | %s" % (line, field_text)

    log.info(line)
    with self.log_file.open("a", encoding="utf-8") as handle:
      handle.write(line + "\n")

  def _switch_label (self, dpid):
    return dpid_to_str(dpid)

  def _link_label (self, link):
    return "%s.%s <-> %s.%s" % (
      self._switch_label(link.dpid1),
      link.port1,
      self._switch_label(link.dpid2),
      link.port2,
    )

  def _flow_delete_count (self):
    cleared = 0
    for connection in self.connections.values():
      clear_msg = of.ofp_flow_mod(command=of.OFPFC_DELETE)
      connection.send(clear_msg)
      cleared += 1
    return cleared

  def _reset_learning_state (self, reason):
    for mac_table in self.mac_to_port.values():
      mac_table.clear()

    cleared = self._flow_delete_count()
    self._log_event(
      "flow_reset",
      "Cleared learned paths after topology change",
      reason=reason,
      switches_cleared=cleared,
    )

  def _physical_links (self):
    links = {}

    for directed in self.directed_links:
      canonical = directed.uni
      details = links.setdefault(
        canonical,
        {
          "label": self._link_label(canonical),
          "forward_seen": False,
          "reverse_seen": False,
        },
      )
      if directed == canonical:
        details["forward_seen"] = True
      if directed == canonical.flipped:
        details["reverse_seen"] = True

    results = []
    for canonical, details in links.items():
      status = "bidirectional" if details["forward_seen"] and details["reverse_seen"] else "partial"
      results.append(
        {
          "dpid1": self._switch_label(canonical.dpid1),
          "port1": canonical.port1,
          "dpid2": self._switch_label(canonical.dpid2),
          "port2": canonical.port2,
          "label": details["label"],
          "status": status,
        }
      )

    return sorted(results, key=lambda item: item["label"])

  def _switch_ports (self, ports):
    physical_ports = []
    for port in ports:
      if port.port_no < of.OFPP_MAX:
        physical_ports.append(int(port.port_no))
    return sorted(physical_ports)

  def _write_state (self):
    state = {
      "generated_at": self._timestamp(),
      "flow_policy": {
        "match_fields": "ofp_match.from_packet(packet, in_port)",
        "unknown_destination_action": "packet_out flood",
        "known_destination_action": "install flow -> output port",
        "idle_timeout_seconds": self.flow_idle_timeout,
        "hard_timeout_seconds": self.flow_hard_timeout,
      },
      "switch_count": len(self.switches),
      "physical_link_count": len(self._physical_links()),
      "host_count": len(self.hosts),
      "switches": [
        {
          "dpid": details["dpid"],
          "ports": details["ports"],
        }
        for _, details in sorted(self.switches.items(), key=lambda item: item[1]["dpid"])
      ],
      "links": self._physical_links(),
      "hosts": [
        {
          "mac": mac,
          "switch": details["switch"],
          "port": details["port"],
          "ip": details.get("ip"),
          "last_seen": details["last_seen"],
        }
        for mac, details in sorted(self.hosts.items())
      ],
    }

    with self.state_file.open("w", encoding="utf-8") as handle:
      json.dump(state, handle, indent=2, sort_keys=False)

  def _update_host_location (self, event, packet):
    if not core.openflow_discovery.is_edge_port(event.dpid, event.port):
      return

    host_mac = str(packet.src)
    host_ip = None

    ipv4_packet = packet.find("ipv4")
    if ipv4_packet is not None:
      host_ip = str(ipv4_packet.srcip)

    arp_packet = packet.find("arp")
    if arp_packet is not None:
      host_ip = str(arp_packet.protosrc)

    new_details = {
      "switch": self._switch_label(event.dpid),
      "port": int(event.port),
      "last_seen": self._timestamp(),
    }
    if host_ip is not None:
      new_details["ip"] = host_ip

    previous = self.hosts.get(host_mac)
    moved = previous is not None and (
      previous["switch"] != new_details["switch"] or previous["port"] != new_details["port"]
    )

    if previous is None:
      self.hosts[host_mac] = new_details
      self._write_state()
      self._log_event(
        "host_discovered",
        "Learned a host on an edge port",
        host=host_mac,
        switch=new_details["switch"],
        port=new_details["port"],
        ip=new_details.get("ip"),
      )
      return

    if moved:
      self.hosts[host_mac] = new_details
      self._write_state()
      self._log_event(
        "host_moved",
        "Host changed attachment point",
        host=host_mac,
        old_switch=previous["switch"],
        old_port=previous["port"],
        new_switch=new_details["switch"],
        new_port=new_details["port"],
        ip=new_details.get("ip"),
      )
      return

    previous["last_seen"] = new_details["last_seen"]
    if host_ip is not None:
      previous["ip"] = host_ip

  def _flood (self, event):
    message = of.ofp_packet_out()
    message.data = event.ofp
    message.in_port = event.port
    message.actions.append(of.ofp_action_output(port=of.OFPP_FLOOD))
    event.connection.send(message)

  def _drop (self, event, duration = None):
    if duration is not None:
      if not isinstance(duration, tuple):
        duration = (duration, duration)

      message = of.ofp_flow_mod()
      message.match = of.ofp_match.from_packet(event.parsed, event.port)
      message.idle_timeout = duration[0]
      message.hard_timeout = duration[1]
      message.buffer_id = event.ofp.buffer_id
      event.connection.send(message)
      return

    if event.ofp.buffer_id is not None:
      message = of.ofp_packet_out()
      message.buffer_id = event.ofp.buffer_id
      message.in_port = event.port
      event.connection.send(message)

  def _install_flow (self, event, packet, out_port):
    message = of.ofp_flow_mod()
    message.match = of.ofp_match.from_packet(packet, event.port)
    message.idle_timeout = self.flow_idle_timeout
    message.hard_timeout = self.flow_hard_timeout
    message.actions.append(of.ofp_action_output(port=out_port))
    message.data = event.ofp
    event.connection.send(message)

    self._log_event(
      "flow_installed",
      "Installed a reactive forwarding rule",
      switch=self._switch_label(event.dpid),
      in_port=event.port,
      out_port=out_port,
      src=packet.src,
      dst=packet.dst,
    )

  def _remove_hosts_on_switch (self, dpid):
    switch_label = self._switch_label(dpid)
    to_remove = [mac for mac, details in self.hosts.items() if details["switch"] == switch_label]
    for mac in to_remove:
      self.hosts.pop(mac, None)

  def _handle_openflow_ConnectionUp (self, event):
    switch_label = self._switch_label(event.dpid)
    self.connections[event.dpid] = event.connection
    self.switches[event.dpid] = {
      "dpid": switch_label,
      "ports": self._switch_ports(event.ofp.ports),
    }
    self.mac_to_port[event.dpid].clear()

    self._write_state()
    self._log_event(
      "switch_connected",
      "Switch connected to the controller",
      switch=switch_label,
      ports=",".join(str(port) for port in self.switches[event.dpid]["ports"]),
    )

  def _handle_openflow_ConnectionDown (self, event):
    switch_label = self._switch_label(event.dpid)
    self.connections.pop(event.dpid, None)
    self.switches.pop(event.dpid, None)
    self.mac_to_port.pop(event.dpid, None)
    self._remove_hosts_on_switch(event.dpid)
    self.directed_links = set(
      link for link in self.directed_links
      if link.dpid1 != event.dpid and link.dpid2 != event.dpid
    )

    self._reset_learning_state("switch_disconnected")
    self._write_state()
    self._log_event(
      "switch_disconnected",
      "Switch disconnected from the controller",
      switch=switch_label,
    )

  def _handle_openflow_PortStatus (self, event):
    switch_label = self._switch_label(event.dpid)
    switch = self.switches.setdefault(
      event.dpid,
      {
        "dpid": switch_label,
        "ports": [],
      },
    )

    port_number = int(event.port)
    if port_number < of.OFPP_MAX:
      if event.added and port_number not in switch["ports"]:
        switch["ports"].append(port_number)
      if event.deleted and port_number in switch["ports"]:
        switch["ports"].remove(port_number)
      switch["ports"] = sorted(switch["ports"])

    if event.added:
      reason = "added"
    elif event.deleted:
      reason = "deleted"
    else:
      reason = "modified"

    self._write_state()
    self._log_event(
      "port_status",
      "Observed a port status change",
      switch=switch_label,
      port=port_number,
      reason=reason,
    )

  def _handle_openflow_discovery_LinkEvent (self, event):
    if event.added:
      self.directed_links.add(event.link)
      action = "link_added"
      message = "Link detected by LLDP discovery"
    else:
      self.directed_links.discard(event.link)
      action = "link_removed"
      message = "Link removed from the topology map"

    self._reset_learning_state(action)
    self._write_state()
    self._log_event(
      action,
      message,
      link=self._link_label(event.link.uni),
      physical_links=len(self._physical_links()),
    )

  def _handle_openflow_PacketIn (self, event):
    packet = event.parsed
    if packet is None or not packet.parsed:
      return

    if packet.type == packet.LLDP_TYPE or packet.dst.isBridgeFiltered():
      self._drop(event)
      return

    self.mac_to_port[event.dpid][packet.src] = event.port
    self._update_host_location(event, packet)

    if packet.dst.is_multicast:
      self._flood(event)
      return

    out_port = self.mac_to_port[event.dpid].get(packet.dst)
    if out_port is None:
      self._flood(event)
      return

    if out_port == event.port:
      self._drop(event, duration=10)
      return

    self._install_flow(event, packet, out_port)


def launch (flow_idle_timeout = 20, flow_hard_timeout = 60,
            log_file = None, state_file = None):
  global log
  log = core.getLogger()

  if not core.hasComponent("openflow_discovery"):
    import pox.openflow.discovery
    pox.openflow.discovery.launch()

  core.registerNew(
    TopologyChangeDetector,
    flow_idle_timeout=flow_idle_timeout,
    flow_hard_timeout=flow_hard_timeout,
    log_file=log_file,
    state_file=state_file,
  )
