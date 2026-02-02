from __future__ import annotations

"""
Mroya Transfer Manager ftrack plugin.

This package registers in ftrack Connect and starts background manager
for component transfers between locations. Manager:

- subscribes to events topic="mroya.transfer.request";
- for each task runs transfer in separate thread;
- publishes standard ftrack.transfer.status events so TransferStatusDialog
  window works in browser and UserTasksWidget.

Connection:
- add this package to ftrack Connect path (via FTRACK_CONNECT_PLUGIN_PATH);
- Connect will automatically find and call register(...) function from hook/transfer_manager.py.
"""

