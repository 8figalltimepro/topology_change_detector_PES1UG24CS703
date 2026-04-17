"""
Top-level POX component entrypoint.
"""

from controller.topology_change_detector import TopologyChangeDetector, launch

__all__ = ["TopologyChangeDetector", "launch"]
