from __future__ import annotations

"""
Background transfer manager for Mroya + dockable UI in ftrack Connect.

- Background manager executes transfers via `mroya.transfer.request` events.
- UI part is added as a separate window / tab in ftrack Connect, using
  the same `TransferStatusDialog` as the browser / UserTasksWidget.
"""

import logging
import logging.handlers
import threading
import queue
import os
import sys
import json
import collections
import time
import socket
from functools import partial
from pathlib import Path
from typing import Any, Dict, Optional

import ftrack_api  # type: ignore
import ftrack_connect.ui.application  # type: ignore
from ftrack_connect.qt import QtWidgets, QtCore, QtGui  # type: ignore

# Force UTF-8 on stdout/stderr (works great on Windows 10/11)
# This fixes encoding issues on systems with problematic default encodings
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
except (AttributeError, ValueError, OSError):
    # reconfigure may not be available or may fail in some contexts
    # (e.g., when stdout is redirected or in certain Python versions)
    pass

try:
    if hasattr(sys.stderr, 'reconfigure'):
        
        sys.stderr.reconfigure(encoding='utf-8')
except (AttributeError, ValueError, OSError):
    # reconfigure may not be available or may fail in some contexts
    pass

def _get_standard_icon(standard_pixmap):
    """Get standard Qt icon with safe error handling."""
    try:
        # Try using QIcon.fromTheme for standard icons
        # This works better than style().standardIcon() in some cases
        icon_names = {
            QtWidgets.QStyle.SP_MediaPause: ['media-playback-pause', 'pause'],
            QtWidgets.QStyle.SP_MediaPlay: ['media-playback-start', 'play'],
            QtWidgets.QStyle.SP_MediaStop: ['media-playback-stop', 'stop'],
        }
        
        if standard_pixmap in icon_names:
            for icon_name in icon_names[standard_pixmap]:
                icon = QtGui.QIcon.fromTheme(icon_name)
                if not icon.isNull():
                    return icon
        
        # Fallback: use standardIcon via QApplication
        app = QtWidgets.QApplication.instance()
        if app and app.style():
            icon = app.style().standardIcon(standard_pixmap)
            if not icon.isNull():
                return icon
    except Exception:
        pass
    
    return QtGui.QIcon()  # Empty icon

def _get_scaled_icon_size(base_size=16):
    """Get icon size accounting for Windows DPI scaling."""
    try:
        app = QtWidgets.QApplication.instance()
        if app:
            # Get devicePixelRatio for DPI scaling
            # Usually 1.0 for 100%, 1.25 for 125%, 1.5 for 150%, 2.0 for 200%
            screen = app.primaryScreen()
            if screen:
                pixel_ratio = screen.devicePixelRatio()
                # For icons use a more conservative approach - don't multiply by pixel_ratio,
                # as Qt already accounts for DPI when rendering icons
                # Just return the base size
                return QtCore.QSize(base_size, base_size)
    except Exception:
        pass
    
    # Fallback: return base size
    return QtCore.QSize(base_size, base_size)

def _get_scaled_button_size(base_size=32):
    """Get button size accounting for Windows DPI scaling."""
    try:
        app = QtWidgets.QApplication.instance()
        if app:
            screen = app.primaryScreen()
            if screen:
                pixel_ratio = screen.devicePixelRatio()
                # For buttons use a more conservative approach
                # Round to nearest integer
                scaled_size = int(base_size * max(1.0, pixel_ratio))
                return QtCore.QSize(scaled_size, scaled_size)
    except Exception:
        pass
    
    return QtCore.QSize(base_size, base_size)

# ---------------------------------------------------------------------------
# Bootstrap sys.path so that sibling package ftrack_inout is importable when
# this plugin is loaded by ftrack Connect.
# ---------------------------------------------------------------------------
_THIS_DIR = Path(__file__).resolve().parent  # .../mroya_transfer_manager-0.1.0/hook
_PLUGINS_ROOT = _THIS_DIR.parent.parent      # .../ftrack_plugins
if str(_PLUGINS_ROOT) not in sys.path:
    sys.path.insert(0, str(_PLUGINS_ROOT))

from ftrack_inout.browser.transfer_status_widget import (  # type: ignore
    TransferStatusDialog,
)
# Helper function for building Ftrack query filter strings
def get_filter_string(entity_ids):
    """Return a comma separated string of quoted ids from *entity_ids* list."""
    return ', '.join('"{0}"'.format(entity_id) for entity_id in entity_ids)

# Import our custom transfer (required, no fallback)
# File is located in hook/lib/ so ftrack doesn't try to load it as a plugin
_import_error = None

# Add dependencies from other plugins to sys.path (e.g., boto3 from multi-site-location)
# This must be done before importing custom_transfer
_plugin_root = _THIS_DIR.parent  # .../mroya_transfer_manager-0.1.0
_plugins_root = _plugin_root.parent  # .../ftrack_plugins

# Add path to mroya_transfer_manager dependencies (fileseq, boto3, etc.)
_transfer_manager_deps_path = _plugin_root / 'dependencies'
if _transfer_manager_deps_path.exists() and str(_transfer_manager_deps_path) not in sys.path:
    sys.path.insert(0, str(_transfer_manager_deps_path))

# Also check ftrack_inout dependencies as fallback
_ftrack_inout_path = _plugins_root / 'ftrack_inout'
_ftrack_inout_deps_path = _ftrack_inout_path / 'dependencies'
if _ftrack_inout_deps_path.exists() and str(_ftrack_inout_deps_path) not in sys.path:
    sys.path.insert(0, str(_ftrack_inout_deps_path))

# Check if boto3 is available in dependencies from other plugins
# For example, in multi-site-location-0.2.0/dependencies
_multi_site_location_path = _plugins_root / 'multi-site-location-0.2.0'
if _multi_site_location_path.exists():
    _dependencies_path = _multi_site_location_path / 'dependencies'
    if _dependencies_path.exists() and str(_dependencies_path) not in sys.path:
        sys.path.insert(0, str(_dependencies_path))

# Add path to our lib before import
lib_path = str(_THIS_DIR / 'lib')
if lib_path not in sys.path:
    sys.path.insert(0, lib_path)

try:
    from custom_transfer import (  # type: ignore
        transfer_component_custom,
    )
except ImportError as e:
    # If import fails, raise error during registration, but don't break module loading
    transfer_component_custom = None
    _import_error = e


logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- 
# Transfer logger setup
# --------------------------------------------------------------------------- 

def _setup_transfer_logger():
    """Set up a separate logger for transfers with file output."""
    transfer_logger = logging.getLogger('mroya_transfer')
    transfer_logger.setLevel(logging.DEBUG)
    
    # Check if already configured
    if transfer_logger.handlers:
        return transfer_logger
    
    # Get ftrack logs directory
    try:
        import platformdirs
        log_dir = platformdirs.user_data_dir('ftrack-connect', 'ftrack', 'log')
    except ImportError:
        # Fallback to local directory
        log_dir = os.path.join(os.path.expanduser('~'), '.ftrack', 'log')
    
    # Create directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Create log file for transfers
    log_file = os.path.join(log_dir, 'mroya_transfer.log')
    
    # Create RotatingFileHandler (max 50MB, 5 backups)
    handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=50 * 1024 * 1024,  # 50MB
        backupCount=5,
        encoding='utf-8'
    )
    handler.setLevel(logging.DEBUG)
    
    # Log format: timestamp | level | job_id | message
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    
    transfer_logger.addHandler(handler)
    transfer_logger.propagate = False  # Don't propagate to main log
    
    return transfer_logger

# Initialize transfer logger
transfer_logger = _setup_transfer_logger()


# --------------------------------------------------------------------------- 
# Helper functions for location type detection and component metadata
# --------------------------------------------------------------------------- 

def get_location_type(location: ftrack_api.entity.base.Entity) -> str:  # type: ignore[name-defined]
    """Determine location type (Disk, S3, etc.)."""
    if not location.accessor:
        return "unknown"
    accessor_type = str(type(location.accessor)).lower()
    if "disk" in accessor_type:
        return "disk"
    elif "s3" in accessor_type:
        return "s3"
    else:
        return "unknown"


def get_component_total_size(component: ftrack_api.entity.Component) -> int:  # type: ignore[name-defined]
    """Get total component size (including sequences)."""
    size = component.get("size") or 0
    return int(size) if size else 0


def _get_components_in_location(session: ftrack_api.Session, entities: list, location: ftrack_api.entity.Location) -> list:  # type: ignore[name-defined]
    """Get list of components from entities (similar to TransferComponentsPlusAction.get_components_in_location)."""
    component_queries = []
    entity_groups = collections.defaultdict(list)
    
    for selection in entities:
        entity_type = selection['entityType']
        entity_id = selection['entityId']
        logger.debug(
            'Processing entity: {0} ({1})'.format(entity_type, entity_id)
        )
        entity_groups[entity_type].append(entity_id)
    
    logger.debug('Entity groups: {0}'.format(entity_groups))
    
    if entity_groups.get('Project'):
        component_queries.append(
            'Component where (version.asset.parent.project.id in ({0}) or '
            'version.asset.parent.id in ({0}))'.format(
                get_filter_string(entity_groups['Project'])
            )
        )

    if entity_groups.get('TypedContext'):
        component_queries.append(
            'Component where (version.asset.parent.ancestors.id in ({0}) or '
            'version.asset.parent.id in ({0}))'.format(
                get_filter_string(entity_groups['TypedContext'])
            )
        )

    if entity_groups.get('assetversion'):
        component_queries.append(
            'Component where version_id in ({0})'.format(
                get_filter_string(entity_groups['assetversion'])
            )
        )

    if entity_groups.get('Component'):
        component_queries.append(
            'Component where id in ({0})'.format(
                get_filter_string(entity_groups['Component'])
            )
        )

    components = set()
    for query_string in component_queries:
        logger.debug('Querying components with: {0}'.format(query_string))
        components.update(
            session.query(query_string).all()
        )

    # Filter sequence members
    # Problem: container_id may not be loaded in query, need to reload components with full data
    # Or use a different approach: query all SequenceComponents and their members separately
    
    # First collect all SequenceComponents and load their members
    sequence_component_ids = set()
    member_ids = set()
    
    for component in components:
        if component.entity_type == 'SequenceComponent':
            sequence_component_ids.add(component['id'])
            # Load component with full data to get members
            try:
                full_component = session.get('Component', component['id'])
                if full_component:
                    members = full_component.get('members', [])
                    if members:
                        for member in members:
                            if member:
                                # member can be an object or string (ID)
                                if isinstance(member, str):
                                    member_ids.add(member)
                                elif hasattr(member, '__getitem__'):
                                    try:
                                        member_id = member['id']
                                        if member_id:
                                            member_ids.add(member_id)
                                    except (KeyError, TypeError):
                                        pass
                                elif hasattr(member, 'id'):
                                    member_ids.add(member.id)
            except Exception as e:
                logger.debug('Error loading sequence members for {0}: {1}'.format(
                    component['id'][:8], e
                ))
    
    # Now filter: exclude FileComponents that are sequence members
    filtered_components = []
    for component in components:
        component_id = component['id']
        
        # If it's a SequenceComponent - add it
        if component.entity_type == 'SequenceComponent':
            filtered_components.append(component)
        # If it's a FileComponent and its ID is in members list - exclude it
        elif component.entity_type == 'FileComponent' and component_id in member_ids:
            logger.debug('Filtering out sequence member FileComponent: {0}'.format(component_id[:8]))
            continue
        # All other components - add them
        else:
            filtered_components.append(component)

    # IMPORTANT: Filter components by location - only include components that are actually in source location
    # This prevents transfer failures when components from ilink are not in the specified source location
    location_filtered_components = []
    for component in filtered_components:
        try:
            # Check if component exists in source location
            component_url = location.get_component_url(component)
            if component_url:
                location_filtered_components.append(component)
                logger.debug('Component {0} found in location {1}'.format(component['id'][:8], location.get('name', 'unknown')))
            else:
                logger.warning(
                    'Component {0} not found in source location {1}, skipping transfer'.format(
                        component['id'][:8],
                        location.get('name', 'unknown')
                    )
                )
        except Exception as exc:
            logger.warning(
                'Error checking component {0} in location {1}: {2}'.format(
                    component['id'][:8] if component else 'unknown',
                    location.get('name', 'unknown'),
                    exc
                )
            )
            # On error, still try to include component (might be a false negative)
            location_filtered_components.append(component)
    
    logger.info('Found {0} components in selection (filtered from {1} total, removed {2} sequence members, {3} not in source location)'.format(
        len(location_filtered_components),
        len(components),
        len(member_ids),
        len(filtered_components) - len(location_filtered_components)
    ))
    
    return location_filtered_components


# --------------------------------------------------------------------------- 
# Background Transfer Manager
# --------------------------------------------------------------------------- 

class _TransferTask:
    """Transfer task."""

    def __init__(self, payload: Dict[str, Any]) -> None:
        self.payload = payload


class BackgroundTransferManager:
    """Background transfer manager running in ftrack Connect process."""

    def __init__(self, session: ftrack_api.Session, max_concurrent_transfers: int = 1) -> None:  # type: ignore[name-defined]
        self._session = session
        self._max_concurrent = max_concurrent_transfers
        self._queue: "queue.Queue[_TransferTask]" = queue.Queue()
        self._active_transfers: Dict[str, threading.Thread] = {}  # {job_id: thread}
        self._active_component_ids: Dict[str, str] = {}  # {component_id: job_id} - for duplicate checking
        self._thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._running = False
        self._transfer_lock = threading.Lock()
        self._transfer_semaphore = threading.Semaphore(max_concurrent_transfers)  # Limit concurrent transfers
        
        # Queue for asynchronous Job updates (to avoid blocking copy thread)
        self._job_update_queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self._job_update_thread = threading.Thread(target=self._job_update_worker, daemon=True)
        self._job_update_thread.start()
        
        # Job status cache for quick checking without blocking copy thread
        # Updated asynchronously in separate thread
        self._job_status_cache: Dict[str, str] = {}  # {job_id: status}
        self._job_status_cache_lock = threading.Lock()
        self._job_status_check_queue: "queue.Queue[str]" = queue.Queue()  # Queue of job_id for status checking
        self._job_status_check_thread = threading.Thread(target=self._job_status_check_worker, daemon=True)
        self._job_status_check_thread.start()
    
    def set_max_concurrent_transfers(self, max_concurrent: int) -> None:
        """Update maximum number of concurrent transfers."""
        if max_concurrent < 1:
            max_concurrent = 1
        if max_concurrent > 10:
            max_concurrent = 10
        
        old_max = self._max_concurrent
        self._max_concurrent = max_concurrent
        
        # If new value is greater than old, release additional slots
        if max_concurrent > old_max:
            for _ in range(max_concurrent - old_max):
                try:
                    self._transfer_semaphore.release()
                except ValueError:
                    # Semaphore already at maximum, ignore
                    pass
        # If new value is less than old, need to decrease semaphore
        # But this is difficult to do safely, so just create a new semaphore
        # (current transfers will continue working)
        elif max_concurrent < old_max:
            # Create new semaphore with new value
            # Current transfers will continue using old semaphore via local references
            # New transfers will use new semaphore
            new_semaphore = threading.Semaphore(max_concurrent)
            # Release slots in new semaphore for already active transfers
            active_count = len(self._active_transfers)
            for _ in range(min(active_count, max_concurrent)):
                try:
                    new_semaphore.release()
                except ValueError:
                    pass
            self._transfer_semaphore = new_semaphore
        
        logger.info(
            "MroyaTransferManager: max_concurrent_transfers changed from %d to %d",
            old_max,
            max_concurrent,
        )

    # ------------------------------------------------------------------ public API

    def register(self) -> None:
        """Subscribe to events and start worker."""
        try:
            logger.info("MroyaTransferManager: registering event listeners...")
            # Ensure event_hub is connected.
            try:
                self._session.event_hub.connect()
            except Exception as exc:  # pragma: no cover
                logger.warning(
                    "MroyaTransferManager: event_hub.connect() failed: %s", exc
                )

            # Subscribe to transfer events only for current user
            # Note: We filter by user only, not by event_hub.id, because different applications
            # (Houdini, Connect, etc.) have different event hub IDs even on the same machine.
            # Additional filtering by hostname/workstation is done in the event handler.
            current_user = self._session.api_user
            current_event_hub_id = self._session.event_hub.id
            current_hostname = socket.gethostname().lower()
            topic_filter = (
                'topic=mroya.transfer.request and '
                'source.user.username="{0}"'
            ).format(current_user)
            logger.info(
                "MroyaTransferManager: Subscribing to topic='mroya.transfer.request' "
                "for user='{0}', hostname='{1}', and event_hub.id='{2}'...".format(
                    current_user, current_hostname, current_event_hub_id
                )
            )
            transfer_logger.info("=" * 80)
            transfer_logger.info("MroyaTransferManager: SUBSCRIPTION SETUP:")
            transfer_logger.info("Current user: %s", current_user)
            transfer_logger.info("Current hostname: %s", current_hostname)
            transfer_logger.info("Current event_hub.id: %s", current_event_hub_id)
            transfer_logger.info("Topic filter: %s", topic_filter)
            transfer_logger.info("=" * 80)
            self._session.event_hub.subscribe(
                topic_filter,
                self._on_transfer_request,
                priority=10,
            )
            logger.info("MroyaTransferManager: Event listeners registered.")
            transfer_logger.info("MroyaTransferManager: Event subscription registered successfully")

            self._running = True
            self._thread.start()
            logger.info("MroyaTransferManager: worker thread started.")
        except Exception as exc:
            logger.error(
                "MroyaTransferManager: failed to register: %s", exc, exc_info=True
            )

    def unregister(self) -> None:
        """Unsubscribe from events and stop worker."""
        logger.info("MroyaTransferManager: unregistering...")
        self._running = False
        try:
            # Unsubscribe with the same filter used during subscription
            current_user = self._session.api_user
            topic_filter = (
                'topic=mroya.transfer.request and '
                'source.user.username="{0}"'
            ).format(current_user)
            self._session.event_hub.unsubscribe(
                topic_filter,
                self._on_transfer_request,
            )
        except Exception:
            pass

    # ------------------------------------------------------------------ event handlers

    def _on_transfer_request(self, event: ftrack_api.event.base.Event) -> None:  # type: ignore[name-defined]
        """Handle `mroya.transfer.request` event.
        
        IMPORTANT: Only process events for current user and current workstation.
        Subscription filter should filter out events from other users and other machines,
        but we add additional validation for reliability.
        """
        try:
            # DETAILED EVENT LOGGING FOR COMPARISON
            current_user_id = self._session.api_user
            current_event_hub_id = self._session.event_hub.id
            
            # Extract all source information for detailed logging
            event_source = event.get("source", {})
            event_source_dict = {}
            if event_source:
                try:
                    # Try to get all available source fields
                    event_source_dict = {
                        'id': event_source.get('id'),
                        'application': event_source.get('application'),
                        'hostname': event_source.get('hostname'),
                        'username': event_source.get('username'),
                    }
                    
                    # Try to get user details if available
                    event_source_user = event_source.get("user")
                    if event_source_user:
                        if isinstance(event_source_user, dict):
                            event_source_dict['user'] = {
                                'username': event_source_user.get('username'),
                                'id': event_source_user.get('id'),
                                'first_name': event_source_user.get('first_name'),
                                'last_name': event_source_user.get('last_name'),
                            }
                        elif hasattr(event_source_user, 'get'):
                            event_source_dict['user'] = {
                                'username': event_source_user.get('username'),
                                'id': event_source_user.get('id'),
                            }
                        else:
                            event_source_dict['user'] = str(event_source_user)
                except Exception as e:
                    event_source_dict['error'] = str(e)
            
            # Get current hostname for comparison
            current_hostname = socket.gethostname().lower()
            
            # Log full event structure for debugging and comparison
            transfer_logger.info("=" * 80)
            transfer_logger.info("EVENT RECEIVED - Full event structure for comparison:")
            transfer_logger.info("Current machine: user=%s, hostname=%s, event_hub.id=%s", current_user_id, current_hostname, current_event_hub_id)
            transfer_logger.info("Event topic: %s", event.get('topic'))
            transfer_logger.info("Event ID: %s", event.get('id'))
            transfer_logger.info("Event source (full): %s", json.dumps(event_source_dict, indent=2, default=str))
            transfer_logger.info("Event data (payload): %s", json.dumps(event.get('data', {}), indent=2, default=str))
            transfer_logger.info("=" * 80)
            
            payload = event["data"]
            job_id = payload.get("job_id")
            user_id = payload.get("user_id")
            job_id_short = job_id[:8] if job_id else "?"
            
            # Additional validation: ensure event is for current user and current machine
            event_source_user = event_source.get("user", {})
            event_source_username = event_source_user.get("username") if isinstance(event_source_user, dict) else None
            event_source_id = event_source.get("id")
            
            # Check user match
            user_matches = False
            if user_id:
                try:
                    user_entity = self._session.get("User", user_id)
                    if user_entity:
                        user_username = user_entity.get("username")
                        if user_username == current_user_id:
                            user_matches = True
                except Exception:
                    pass
            
            # If user_id not specified or doesn't match, check source.user from event
            if not user_matches and event_source_username:
                if event_source_username == current_user_id:
                    user_matches = True
            
            # Check workstation match by hostname
            # Note: We don't check event_hub.id because different applications (Houdini, Connect)
            # have different event hub IDs even on the same machine.
            # IMPORTANT: hostname is now always set when publishing events from browser,
            # so we require it to be present for proper workstation filtering.
            event_hostname = event_source.get("hostname")
            host_matches = False  # Default to False - require hostname match
            
            if event_hostname:
                event_hostname = str(event_hostname).lower()
                if event_hostname == current_hostname:
                    host_matches = True
            
            # IMPORTANT: Require BOTH user match AND hostname match
            # This prevents processing events from other users on other machines
            # even if they have the same username (which shouldn't happen, but safety first)
            
            # If unable to determine user, log warning and skip
            if not user_matches and (user_id or event_source_username):
                transfer_logger.warning("=" * 80)
                transfer_logger.warning(
                    "EVENT REJECTED - Different user detected:"
                )
                transfer_logger.warning(
                    "Current machine: user=%s, hostname=%s, event_hub.id=%s",
                    current_user_id,
                    current_hostname,
                    current_event_hub_id
                )
                transfer_logger.warning(
                    "Event source: %s",
                    json.dumps(event_source_dict, indent=2, default=str)
                )
                transfer_logger.warning(
                    "Event payload user_id: %s, event_source_user: %s",
                    user_id,
                    event_source_username
                )
                transfer_logger.warning(
                    "Event hostname: %s, current hostname: %s",
                    event_source_dict.get('hostname', 'NOT SET'),
                    current_hostname
                )
                transfer_logger.warning("=" * 80)
                logger.warning(
                    "MroyaTransferManager: _on_transfer_request REJECTED - Different user "
                    "(job_id=%s, payload_user_id=%s, event_source_user=%s, current_user=%s, "
                    "event_hostname=%s, current_hostname=%s). Skipping to prevent cross-user task execution.",
                    job_id_short,
                    user_id,
                    event_source_username,
                    current_user_id,
                    event_source_dict.get('hostname', 'NOT SET'),
                    current_hostname
                )
                return
            
            # Reject events without hostname or with mismatched hostname
            # This is the SECOND check - even if user matches, hostname must also match
            if not host_matches:
                transfer_logger.warning("=" * 80)
                if not event_hostname:
                    transfer_logger.warning(
                        "EVENT REJECTED - Missing hostname in event source:"
                    )
                else:
                    transfer_logger.warning(
                        "EVENT REJECTED - Different machine detected:"
                    )
                transfer_logger.warning(
                    "Current machine: user=%s, hostname=%s, event_hub.id=%s",
                    current_user_id,
                    current_hostname,
                    current_event_hub_id
                )
                transfer_logger.warning(
                    "Event source: %s",
                    json.dumps(event_source_dict, indent=2, default=str)
                )
                transfer_logger.warning(
                    "Event payload user_id: %s, event_source_user: %s",
                    user_id,
                    event_source_username
                )
                transfer_logger.warning(
                    "Event hostname: %s, current hostname: %s",
                    event_hostname or "NOT SET",
                    current_hostname
                )
                transfer_logger.warning("=" * 80)
                transfer_logger.warning(
                    "Event source: %s",
                    json.dumps(event_source_dict, indent=2, default=str)
                )
                transfer_logger.warning(
                    "Event hostname: %s, current hostname: %s",
                    event_hostname or "(not set)",
                    current_hostname
                )
                transfer_logger.warning("=" * 80)
                logger.warning(
                    "MroyaTransferManager: _on_transfer_request REJECTED - Different/missing hostname "
                    "(job_id=%s, event_hostname=%s, current_hostname=%s, "
                    "event_user=%s, current_user=%s). Skipping to prevent cross-machine task execution.",
                    job_id_short,
                    event_hostname or "NOT SET",
                    current_hostname,
                    event_source_username or user_id or "UNKNOWN",
                    current_user_id
                )
                return
            
            # Both user and hostname matched - this event is for us
            transfer_logger.info("=" * 80)
            transfer_logger.info("EVENT ACCEPTED - User and hostname match:")
            transfer_logger.info(
                "Current machine: user=%s, hostname=%s, event_hub.id=%s",
                current_user_id,
                current_hostname,
                current_event_hub_id
            )
            transfer_logger.info(
                "Event source: %s",
                json.dumps(event_source_dict, indent=2, default=str)
            )
            transfer_logger.info("=" * 80)
            
            logger.info(
                "MroyaTransferManager: _on_transfer_request received (job_id=%s, user=%s, hostname=%s, queue_size=%d)",
                job_id_short,
                current_user_id,
                current_hostname,
                self._queue.qsize(),
            )
            task = _TransferTask(payload)
            self._queue.put(task)
            logger.info(
                "MroyaTransferManager: transfer request queued (job_id=%s, queue_size=%d)",
                job_id_short,
                self._queue.qsize(),
            )
        except Exception as exc:
            logger.error(
                "MroyaTransferManager: failed to queue transfer request: %s",
                exc,
                exc_info=True,
            )

    # ------------------------------------------------------------------ worker loop

    def _worker_loop(self) -> None:
        """Main worker loop processing task queue."""
        logger.info("MroyaTransferManager: worker loop started")
        while self._running:
            try:
                task = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue

            job_id = task.payload.get("job_id")
            job_id_short = job_id[:8] if job_id else "?"
            logger.info(
                "MroyaTransferManager: _worker_loop got task (job_id=%s, queue_size=%d, active_transfers=%d)",
                job_id_short,
                self._queue.qsize(),
                len(self._active_transfers),
            )
            
            if job_id:
                # Check job status and settings
                try:
                    job = self._session.get('Job', job_id)
                    if job:
                        status = job.get('status')
                        # If paused, skip (wait for resume)
                        if status == 'paused':
                            logger.info(
                                "MroyaTransferManager: job %s is paused, skipping",
                                job_id_short,
                            )
                            self._queue.task_done()
                            continue
                        
                        # Check auto_start setting
                        job_data = json.loads(job.get('data', '{}') or '{}')
                        auto_start = job_data.get('auto_start', True)
                        if not auto_start and status == 'queued':
                            logger.info(
                                "MroyaTransferManager: job %s has auto_start=False, skipping",
                                job_id_short,
                            )
                            self._queue.task_done()
                            continue
                except Exception:
                    pass
                
                # Check if there's already an active transfer with this job_id
                with self._transfer_lock:
                    if job_id in self._active_transfers:
                        logger.warning(
                            "MroyaTransferManager: transfer for job %s already active, skipping",
                            job_id_short,
                        )
                        self._queue.task_done()
                        continue
                
                # Acquire semaphore (block if there's already an active transfer)
                # This ensures transfers execute sequentially
                logger.debug(
                    "MroyaTransferManager: acquiring semaphore for job %s (active_transfers=%d)",
                    job_id_short,
                    len(self._active_transfers),
                )
                self._transfer_semaphore.acquire()
                logger.debug(
                    "MroyaTransferManager: semaphore acquired for job %s",
                    job_id_short,
                )
                
                # Start transfer in separate thread
                thread = threading.Thread(
                    target=self._run_transfer,
                    args=(task,),
                    daemon=True,
                )
                with self._transfer_lock:
                    self._active_transfers[job_id] = thread
                thread.start()
                # Don't call task_done here - _run_transfer will do it after completion
            else:
                # If no job_id, acquire semaphore and run directly
                self._transfer_semaphore.acquire()
                try:
                    self._run_transfer(task)
                finally:
                    self._transfer_semaphore.release()
                    self._queue.task_done()

    def _job_update_worker(self) -> None:
        """Worker for asynchronous Job updates in separate thread.
        
        This allows avoiding blocking the file copy thread with session.commit() calls.
        """
        while self._running:
            try:
                # Get Job update task (with timeout to check self._running)
                update_data = self._job_update_queue.get(timeout=1.0)
                
                job_id = update_data.get('job_id')
                job_data_dict = update_data.get('job_data')
                
                if job_id and job_data_dict:
                    try:
                        # Get Job and update its data
                        job = self._session.get('Job', job_id)
                        if job:
                            job['data'] = json.dumps(job_data_dict)
                            self._session.commit()
                            logger.debug(f"Job {job_id[:8]} updated asynchronously")
                    except Exception as exc:
                        logger.warning(f"Failed to update job {job_id[:8]} asynchronously: {exc}")
                
                self._job_update_queue.task_done()
            except queue.Empty:
                continue
            except Exception as exc:
                logger.error(f"Error in job update worker: {exc}", exc_info=True)
    
    def _job_status_check_worker(self) -> None:
        """Worker for asynchronous Job status checking in separate thread.
        
        Updates status cache so copy thread can quickly check status
        without blocking session.get() calls.
        """
        while self._running:
            try:
                # Get job_id for status check (with timeout)
                job_id = self._job_status_check_queue.get(timeout=1.0)
                
                if job_id:
                    try:
                        # Get Job status from Ftrack
                        job = self._session.get('Job', job_id)
                        if job:
                            status = job.get('status', 'unknown')
                            # Update cache
                            with self._job_status_cache_lock:
                                self._job_status_cache[job_id] = status
                    except Exception as exc:
                        logger.warning(f"Failed to check job status {job_id[:8]}: {exc}")
                
                self._job_status_check_queue.task_done()
            except queue.Empty:
                continue
            except Exception as exc:
                logger.error(f"Error in job status check worker: {exc}", exc_info=True)
    
    def _get_job_status_cached(self, job_id: str) -> Optional[str]:
        """Get Job status from cache (fast, non-blocking).
        
        If status not in cache, adds job_id to queue for checking.
        """
        with self._job_status_cache_lock:
            status = self._job_status_cache.get(job_id)
        
        # If status not in cache, add to queue for checking
        if status is None:
            try:
                self._job_status_check_queue.put(job_id, block=False)
            except queue.Full:
                pass
        
        return status
    
    def _request_job_status_check(self, job_id: str) -> None:
        """Request Job status check (asynchronously, non-blocking)."""
        try:
            self._job_status_check_queue.put(job_id, block=False)
        except queue.Full:
            pass
    
    def _update_job_async(self, job_id: str, job_data: Dict[str, Any]) -> None:
        """Add task for asynchronous Job update.
        
        This does NOT block the file copy thread.
        """
        try:
            self._job_update_queue.put({
                'job_id': job_id,
                'job_data': job_data
            }, block=False)
        except queue.Full:
            logger.warning(f"Job update queue full, skipping update for job {job_id[:8]}")
        except Exception as exc:
            logger.warning(f"Failed to queue job update: {exc}")
    
    def _run_transfer(self, task: _TransferTask) -> None:
        """Start transfer in separate thread.
        
        IMPORTANT: Session is NOT needed for file copying itself!
        Session is only used for:
        1. Getting metadata (Location, Component) - once at the start
        2. Updating Job (progress) - can be done less frequently or via events
        3. Registering component in target location - once at the end
        
        Therefore we create session only for these operations, not for the entire transfer process.
        """
        try:
            # Pass task to _process_task
            # Session will be created only when needed for metadata and finalization
            self._process_task(task)
        except Exception as exc:
            logger.error("MroyaTransferManager: error in transfer thread: %s", exc, exc_info=True)
        finally:
            # Release semaphore and clear active transfers
            job_id = task.payload.get("job_id")
            if job_id:
                logger.debug("MroyaTransferManager: transfer thread for job %s exiting", job_id[:8])
                with self._transfer_lock:
                    self._active_transfers.pop(job_id, None)
            # Release semaphore in any case
            self._transfer_semaphore.release()

    def _process_task(self, task: _TransferTask) -> None:
        """Process transfer task using custom transfer.
        
        IMPORTANT: Session is only used for getting metadata and updating Job.
        Session is NOT needed for file copying itself - these are pure file operations.
        """
        # Use main session only for metadata and Job updates
        # Session is NOT needed for file transfer itself
        
        data = task.payload
        selection = data.get("selection") or []
        from_location_id = data.get("from_location_id")
        to_location_id = data.get("to_location_id")
        user_id = data.get("user_id")
        job_id = data.get("job_id")

        logger.info(
            "MroyaTransferManager: processing transfer request (custom): from=%s to=%s, "
            "selection=%r, job_id=%r",
            from_location_id,
            to_location_id,
            selection,
            job_id,
        )

        if not from_location_id or not to_location_id or not selection:
            logger.warning(
                "MroyaTransferManager: invalid transfer request, missing "
                "from/to/selection: %r",
                data,
            )
            return

        # Get Location and, if needed, existing Job.
        # This is the only place where session is needed for metadata
        try:
            src_location = self._session.get("Location", str(from_location_id))
            dst_location = self._session.get("Location", str(to_location_id))
        except Exception as exc:
            logger.error("MroyaTransferManager: failed to resolve locations: %s", exc, exc_info=True)
            return

        if not src_location or not dst_location:
            logger.warning(
                "MroyaTransferManager: source or target location not found "
                "(src=%r, dst=%r)",
                src_location,
                dst_location,
            )
            return

        # Get or create Job (for progress tracking)
        job = None
        if job_id:
            try:
                job = self._session.get("Job", str(job_id))
            except Exception as exc:
                logger.warning(
                    "MroyaTransferManager: failed to fetch pre-created Job %s: %s",
                    job_id,
                    exc,
                )
                job = None
        
        if not job:
            try:
                job = self._session.create(
                    'Job',
                    {
                        'user_id': user_id,
                        'status': 'running',
                        'data': json.dumps({
                            'description': 'Transfer components (Gathering...)'
                        }),
                    },
                )
                self._session.commit()
            except Exception as exc:
                logger.error("MroyaTransferManager: failed to create Job: %s", exc, exc_info=True)
                return

        try:
            # Update Job status
            job['status'] = 'running'
            job_data = json.loads(job.get('data', '{}') or '{}')
            job['data'] = json.dumps(job_data)
            self._session.commit()
        except Exception:
            pass

        # Get components from selection (session only needed for metadata)
        try:
            logger.info(
                "MroyaTransferManager: Getting components from selection (count=%d) for source location '%s' (%s)",
                len(selection),
                src_location.get('name', 'unknown'),
                src_location.get('id', 'unknown')[:8] if src_location else 'None'
            )
            components = _get_components_in_location(self._session, selection, src_location)
            logger.info(
                "MroyaTransferManager: Found %d components in source location '%s'",
                len(components) if components else 0,
                src_location.get('name', 'unknown')
            )
        except Exception as exc:
            logger.error("MroyaTransferManager: failed to get components: %s", exc, exc_info=True)
            job['status'] = 'failed'
            try:
                job_data = json.loads(job.get('data', '{}') or '{}')
                job_data['description'] = f'Failed to get components: {exc}'
                job['data'] = json.dumps(job_data)
                self._session.commit()
            except Exception:
                pass
            return

        if not components:
            logger.warning(
                "MroyaTransferManager: no components found in selection for source location '%s' (%s). "
                "Selection entities: %r",
                src_location.get('name', 'unknown'),
                src_location.get('id', 'unknown')[:8] if src_location else 'None',
                selection
            )
            job['status'] = 'failed'
            try:
                job_data = json.loads(job.get('data', '{}') or '{}')
                job_data['description'] = f'No components found in source location {src_location.get("name", "unknown")}'
                job['data'] = json.dumps(job_data)
                self._session.commit()
            except Exception:
                pass
            return

        # Update Job description and set start time
        transfer_start_time = time.time()
        try:
            job_data = json.loads(job.get('data', '{}') or '{}')
            job_data['description'] = (
                'Transferring {0} components from {1} to {2}'.format(
                    len(components),
                    src_location['name'],
                    dst_location['name'],
                )
            )
            job_data['progress'] = 0.0
            job_data['start_time'] = transfer_start_time
            job_data['elapsed_time'] = 0.0
            job_data['speed_mbps'] = None
            # Save selection for resume capability
            job_data['selection'] = selection
            job_data['from_location_id'] = from_location_id
            job_data['to_location_id'] = to_location_id
            # Default settings (can be changed via UI)
            # Load max_workers from settings if not specified in job_data
            # IMPORTANT: Don't use QtCore.QSettings in background thread - it can block!
            # Use value from job_data or default
            if 'max_workers' not in job_data:
                job_data['max_workers'] = 10  # Default
            job_data['auto_start'] = job_data.get('auto_start', True)  # Auto-start by default
            job['data'] = json.dumps(job_data)
            self._session.commit()
            
            # Log transfer start
            transfer_logger.info(
                f"JOB_START | job_id={job['id']} | "
                f"components={len(components)} | "
                f"from={src_location['name']} | to={dst_location['name']}"
            )
        except Exception:
            pass

        # Publish transfer start event via main session (event_hub must be connected)
        # Use main session for publishing events, as event_hub may not be connected in new session
        # IMPORTANT: Include hostname in source for proper filtering in MroyaTransferManagerWidget._handler
        try:
            import socket
            current_hostname = socket.gethostname().lower()
            self._session.event_hub.publish(
                ftrack_api.event.base.Event(
                    topic='ftrack.transfer.status',
                    data={'job_id': job['id'], 'status': 'running', 'progress': 0.0},
                    source={'hostname': current_hostname}
                ),
                on_error='ignore'
            )
        except Exception:
            pass

        # Transfer components
        ignore_errors = bool(data.get("ignore_location_errors", False))
        total_components = len(components)
        success_count = 0
        failed_count = 0
        
        # Overall progress: account for progress of all components
        # For each component progress is 0 to 1, overall = (completed + current_progress) / total
        # Use list for mutable object (so it can be modified from closure)
        completed_components = [0]
        
        # For calculating overall speed, accumulate total bytes transferred
        total_bytes_transferred_all = 0

        for idx, component in enumerate(components, start=1):
            logger.info('Transfering component: {0}'.format(component))
            
            # Check if this component is already being processed in another job
            component_id = component['id']
            with self._transfer_lock:
                if component_id in self._active_component_ids:
                    existing_job_id = self._active_component_ids[component_id]
                    logger.warning(
                        "MroyaTransferManager: component %s already being transferred in job %s, marking as failed",
                        component_id[:8],
                        existing_job_id[:8] if existing_job_id else "?",
                    )
                    # Mark job as failed
                    job['status'] = 'failed'
                    try:
                        job_data = json.loads(job.get('data', '{}') or '{}')
                        job_data['description'] = f'Component {component_id[:8]} already being transferred in another job'
                        job['data'] = json.dumps(job_data)
                        self._session.commit()
                    except Exception:
                        pass
                    failed_count += 1
                    continue
                
                # Register component as active
                self._active_component_ids[component_id] = job['id']
            
            try:
                # Get current job_data to pass to transfer_component_custom
                job_data = json.loads(job.get('data', '{}') or '{}')
                
                # Create progress callback
                # IMPORTANT: Progress should be from total component size (file or sequence)
                last_progress_event = [0.0, 0.0]  # [component_progress, elapsed_time]
                last_commit_time = [transfer_start_time]  # Time of last commit
                last_log_time = [transfer_start_time]  # Time of last log
                last_publish_time = [transfer_start_time]  # Time of last event publish
                
                # Cache for status checking (updated asynchronously in separate thread)
                last_status_check_time = [time.time()]  # Time of last check
                
                # Initialize status cache for this job
                self._request_job_status_check(job['id'])
                
                def progress_callback(bytes_transferred, total_size):
                    """
                    Callback for current component progress.
                    
                    bytes_transferred: number of bytes transferred for current component
                    total_size: total size of current component (file or sequence)
                    
                    Calculate:
                    - component_progress: progress of current component (0.0 - 1.0)
                    - overall_progress: overall progress of all components (0.0 - 1.0)
                    """
                    # Get current time ONCE for all calculations
                    current_time = time.time()
                    
                    # Check if job was stopped or paused by user
                    # Use status cache (updated asynchronously) - does NOT block copy thread!
                    should_request_check = current_time - last_status_check_time[0] >= 1.0
                    
                    if should_request_check:
                        # Request status update (asynchronously, non-blocking)
                        self._request_job_status_check(job['id'])
                        last_status_check_time[0] = current_time
                    
                    # Get status from cache (fast, non-blocking)
                    status = self._get_job_status_cached(job['id'])
                    
                    # If status not yet cached, use default 'running'
                    if status is None:
                        status = 'running'
                    
                    # Check status and interrupt transfer if needed
                    if status == 'killed':
                        raise InterruptedError("Transfer cancelled by user")
                    elif status == 'paused':
                        raise InterruptedError("Transfer paused by user")
                    
                    if total_size > 0:
                        # Current component progress (0.0 - 1.0)
                        component_progress = float(bytes_transferred) / float(total_size)
                        
                        # Overall progress of all components
                        # Formula: (completed_components[0] + component_progress) / total_components
                        if total_components > 0:
                            overall_progress = (completed_components[0] + component_progress) / float(total_components)
                        else:
                            overall_progress = component_progress
                        
                        # Calculate time from start of all components transfer
                        elapsed_time = current_time - transfer_start_time
                        
                        # Calculate speed from total bytes transferred
                        # Accumulate bytes: previous components + current component
                        # IMPORTANT: bytes_transferred is progress of current component
                        # Need to calculate total bytes:
                        # - completed_components already transferred their bytes
                        # - current component transferred bytes_transferred from total_size
                        # But we don't know sizes of previous components...
                        # For now use simplified approach: speed from current component
                        # TODO: Can improve by accumulating total_bytes of each component
                        speed_mbps = None
                        if elapsed_time > 0:
                            # Speed in MB/s from transfer start
                            # Use bytes_transferred for current component
                            # This gives instantaneous speed, not average from all components
                            speed_mbps = (bytes_transferred / (1024 * 1024)) / elapsed_time
                        
                        # Update job_data in memory (fast)
                        # Use overall_progress for UI display
                        job_data['progress'] = overall_progress
                        job_data['elapsed_time'] = elapsed_time
                        job_data['speed_mbps'] = speed_mbps
                        
                        # Commit to DB less frequently - every 5 seconds or every 10%
                        # This is critical for performance, as commit blocks thread
                        last_committed_progress = last_progress_event[0]
                        should_commit = (
                            current_time - last_commit_time[0] >= 5.0 or  # Every 5 seconds
                            abs(overall_progress - last_committed_progress) >= 0.10  # Or every 10% overall progress
                        )
                        
                        if should_commit:
                            # Update Job asynchronously to avoid blocking copy thread
                            self._update_job_async(job['id'], job_data.copy())
                            last_commit_time[0] = current_time
                        
                        # Log progress (every 10% overall progress or every 5 seconds)
                        # Use separate variable for last log time
                        last_logged_progress = last_progress_event[0]
                        time_since_last_log = current_time - last_log_time[0]
                        should_log = (
                            abs(overall_progress - last_logged_progress) >= 0.10 or  # Every 10% overall progress
                            time_since_last_log >= 5.0  # Or every 5 seconds
                        )
                        
                        if should_log:
                            last_progress_event[0] = overall_progress
                            last_progress_event[1] = elapsed_time
                            last_log_time[0] = current_time
                            
                            transfer_logger.info(
                                f"JOB_PROGRESS | job_id={job['id']} | "
                                f"overall_progress={overall_progress*100:.1f}% | "
                                f"component_progress={component_progress*100:.1f}% | "
                                f"component_bytes={bytes_transferred}/{total_size} | "
                                f"time={elapsed_time:.1f}s | "
                                f"speed={speed_mbps:.2f} MB/s" if speed_mbps else "speed=N/A"
                            )
                        
                        # Publish progress event with fixed frequency (every 1.5 seconds)
                        # This ensures stable UI updates without jumps
                        time_since_last_publish = current_time - last_publish_time[0]
                        should_publish = (
                            time_since_last_publish >= 1.5 or  # Fixed frequency: every 1.5 seconds
                            overall_progress >= 1.0  # Or on completion
                        )
                        
                        if should_publish:
                            last_publish_time[0] = current_time
                            try:
                                # Publish event asynchronously to avoid blocking transfer thread
                                # Include time and speed in event for UI updates
                                # Use overall_progress for UI display
                                # IMPORTANT: Include hostname in source for proper filtering
                                import socket
                                current_hostname = socket.gethostname().lower()
                                self._session.event_hub.publish(
                                    ftrack_api.event.base.Event(
                                        topic='ftrack.transfer.status',
                                        data={
                                            'job_id': job['id'],
                                            'status': 'running',
                                            'progress': overall_progress,  # Overall progress of all components
                                            'elapsed_time': elapsed_time,
                                            'speed_mbps': speed_mbps,
                                        },
                                        source={'hostname': current_hostname}
                                    ),
                                    on_error='ignore'
                                )
                            except Exception:
                                pass
                
                # Call custom transfer
                success = transfer_component_custom(
                    session=self._session,
                    component=component,
                    source_location=src_location,
                    target_location=dst_location,
                    job_data=job_data,
                    progress_callback=progress_callback
                )
                
                # Final job_data update after transfer completion
                try:
                    job['data'] = json.dumps(job_data)
                    self._session.commit()
                except Exception:
                    pass
                
                if success:
                    success_count += 1
                    # Increment completed components counter for next component
                    completed_components[0] += 1
                else:
                    failed_count += 1
                    # Even on error, component is considered processed
                    completed_components[0] += 1
                    if not ignore_errors:
                        raise Exception(f"Failed to transfer component {component['id']}")
            except InterruptedError:
                # Transfer was stopped by user
                logger.info('Transfer cancelled by user (InterruptedError)')
                transfer_logger.info(f"JOB_CANCELLED | job_id={job['id']} | component transfer cancelled")
                # Don't increment failed_count, just exit
                return
            except Exception as exc:
                logger.exception('Failed to transfer component.')
                
                # Log error to transfer_logger for detailed analysis
                import traceback
                error_details = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
                transfer_logger.error(
                    f"COMPONENT_TRANSFER_ERROR | job_id={job['id']} | "
                    f"component_id={component.get('id', 'unknown')[:8]} | "
                    f"error={str(exc)} | "
                    f"traceback={error_details[:500]}"  # First 500 characters of traceback
                )
                
                # Check if Job was stopped by user (use cache, non-blocking)
                status = self._get_job_status_cached(job['id'])
                if status == 'killed':
                    # Job was stopped by user - don't update status
                    transfer_logger.info(f"JOB_CANCELLED | job_id={job['id']} | component transfer cancelled")
                    return
                
                failed_count += 1
                if not ignore_errors:
                    # Update Job with error
                    job['status'] = 'failed'
                    try:
                        job_data = json.loads(job.get('data', '{}') or '{}')
                        job_data['description'] = f'Failed to transfer component: {exc}'
                        job['data'] = json.dumps(job_data)
                        self._session.commit()
                    except Exception:
                        pass
                    return
            finally:
                # Remove component from active after transfer completion (successful or failed)
                with self._transfer_lock:
                    self._active_component_ids.pop(component_id, None)
            
            # After component completion, progress was already updated in progress_callback
            # Don't duplicate update to avoid progress jumps

        # Finish Job
        transfer_end_time = time.time()
        final_elapsed_time = transfer_end_time - transfer_start_time
        
        if failed_count == 0:
            # Before setting status to 'done', publish final event with progress=1.0
            # This prevents progress jump from 100% to 0%
            try:
                # Publish event with progress=1.0 and status 'running' (still)
                # IMPORTANT: Include hostname in source for proper filtering
                import socket
                current_hostname = socket.gethostname().lower()
                self._session.event_hub.publish(
                    ftrack_api.event.base.Event(
                        topic='ftrack.transfer.status',
                        data={
                            'job_id': job['id'],
                            'status': 'running',
                            'progress': 1.0,  # Final progress 100%
                            'elapsed_time': final_elapsed_time,
                            'speed_mbps': None,  # Speed will be calculated below
                        },
                        source={'hostname': current_hostname}
                    ),
                    on_error='ignore'
                )
                
                # Calculate final speed
                job_data = json.loads(job.get('data', '{}') or '{}')
                total_bytes = job_data.get('total_size_bytes', 0)
                final_speed = None
                if total_bytes > 0 and final_elapsed_time > 0:
                    final_speed = (total_bytes / (1024 * 1024)) / final_elapsed_time
                
                # Publish another event with final speed and status 'done'
                # IMPORTANT: Include hostname in source for proper filtering
                import socket
                current_hostname = socket.gethostname().lower()
                self._session.event_hub.publish(
                    ftrack_api.event.base.Event(
                        topic='ftrack.transfer.status',
                        data={
                            'job_id': job['id'],
                            'status': 'done',
                            'progress': 1.0,  # Progress remains 100%
                            'elapsed_time': final_elapsed_time,
                            'speed_mbps': final_speed,
                        },
                        source={'hostname': current_hostname}
                    ),
                    on_error='ignore'
                )
            except Exception:
                pass
            
            job['status'] = 'done'
            try:
                job_data = json.loads(job.get('data', '{}') or '{}')
                job_data['description'] = f'Transferred {success_count} components successfully'
                job_data['progress'] = 1.0
                job_data['elapsed_time'] = final_elapsed_time
                # Final speed
                total_bytes = job_data.get('total_size_bytes', 0)
                if total_bytes > 0 and final_elapsed_time > 0:
                    job_data['speed_mbps'] = (total_bytes / (1024 * 1024)) / final_elapsed_time
                job['data'] = json.dumps(job_data)
                self._session.commit()
                
                # Log completion
                transfer_logger.info(
                    f"JOB_DONE | job_id={job['id']} | "
                    f"components={success_count} | "
                    f"time={final_elapsed_time:.1f}s | "
                    f"avg_speed={job_data.get('speed_mbps', 0):.2f} MB/s" if job_data.get('speed_mbps') else "avg_speed=N/A"
                )
            except Exception:
                pass
        else:
            job['status'] = 'failed' if not ignore_errors else 'done'
            try:
                job_data = json.loads(job.get('data', '{}') or '{}')
                job_data['description'] = f'Transferred {success_count}/{len(components)} components ({failed_count} failed)'
                job_data['elapsed_time'] = final_elapsed_time
                job['data'] = json.dumps(job_data)
                self._session.commit()
                
                # Log error
                transfer_logger.warning(
                    f"JOB_FAILED | job_id={job['id']} | "
                    f"success={success_count}/{len(components)} | "
                    f"failed={failed_count} | "
                    f"time={final_elapsed_time:.1f}s"
                )
            except Exception:
                pass

        # Final event for success case already published above (with progress=1.0 and status 'done')
        # For failed case, publish event here if it hasn't been published yet
        if failed_count > 0:
            try:
                # IMPORTANT: Include hostname in source for proper filtering
                import socket
                current_hostname = socket.gethostname().lower()
                job_data = json.loads(job.get('data', '{}') or '{}')
                self._session.event_hub.publish(
                    ftrack_api.event.base.Event(
                        topic='ftrack.transfer.status',
                        data={
                            'job_id': job['id'],
                            'status': job['status'],
                            'progress': job_data.get('progress', 0.0),
                            'elapsed_time': final_elapsed_time,
                        },
                        source={'hostname': current_hostname}
                    ),
                    on_error='ignore'
                )
            except Exception:
                pass

class MroyaTransferManagerWidget(ftrack_connect.ui.application.ConnectWidget):
    """Dockable widget in ftrack Connect hosting the shared Transfer Manager."""

    icon = ":ftrack/image/default/ftrackLogoColor"
    name = "Mroya Transfer Manager"
    
    _global_manager = None  # Reference to BackgroundTransferManager

    def __init__(self, session, parent=None):
        """Initialize the widget."""
        super(MroyaTransferManagerWidget, self).__init__(session, parent=parent)
        
        # Create layout
        layout = QtWidgets.QVBoxLayout()
        self.setLayout(layout)
        
        # Settings panel
        settings_panel = QtWidgets.QWidget()
        settings_layout = QtWidgets.QHBoxLayout(settings_panel)
        settings_layout.setContentsMargins(5, 5, 5, 5)
        
        # Max concurrent transfers setting
        max_concurrent_label = QtWidgets.QLabel("Max concurrent transfers:")
        settings_layout.addWidget(max_concurrent_label)
        
        self.max_concurrent_spinbox = QtWidgets.QSpinBox()
        self.max_concurrent_spinbox.setMinimum(1)
        self.max_concurrent_spinbox.setMaximum(10)
        self.max_concurrent_spinbox.setValue(1)
        self.max_concurrent_spinbox.valueChanged.connect(self._on_max_concurrent_changed)
        settings_layout.addWidget(self.max_concurrent_spinbox)
        
        # Max workers (threads per sequence) setting
        max_workers_label = QtWidgets.QLabel("Max workers (threads per sequence):")
        settings_layout.addWidget(max_workers_label)
        
        self.max_workers_spinbox = QtWidgets.QSpinBox()
        self.max_workers_spinbox.setMinimum(1)
        self.max_workers_spinbox.setMaximum(20)
        self.max_workers_spinbox.setValue(10)
        self.max_workers_spinbox.valueChanged.connect(self._on_max_workers_changed)
        settings_layout.addWidget(self.max_workers_spinbox)
        
        settings_layout.addStretch()
        layout.addWidget(settings_panel)
        
        # Create simple table widget for jobs
        self.job_table = QtWidgets.QTableWidget()
        self.job_table.setColumnCount(8)
        self.job_table.setHorizontalHeaderLabels([
            "Component", "Destination", "Size", "Status", "Progress", "Time", "Speed", "Actions"
        ])
        self.job_table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        self.job_table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.job_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        
        # Add styles for table so colors work correctly
        self.job_table.setStyleSheet("""
            QTableWidget {
                gridline-color: #333333;
            }
            QTableWidget::item {
                padding: 3px;
            }
            QTableWidget::item:selected {
                background-color: #4682B4;
                color: white;
            }
        """)
        
        layout.addWidget(self.job_table)
        
        # Load settings
        self._settings = QtCore.QSettings("mroya", "TransferManager")
        max_concurrent = self._settings.value("max_concurrent_transfers", 1, type=int)
        self.max_concurrent_spinbox.setValue(max_concurrent)
        
        max_workers = self._settings.value("max_workers", 10, type=int)
        self.max_workers_spinbox.setValue(max_workers)
        
        # Track active jobs
        # Structure: {job_id: {'row': int, 'start_time': float, 'last_elapsed_time': float, 'last_speed': float}}
        self.active_jobs = {}
        self._filter_tag = "mroya_transfer"
        
        # Timer for polling job statuses
        self.poll_timer = QtCore.QTimer(self)
        self.poll_timer.timeout.connect(self._check_job_statuses)
        self.poll_timer.start(5000)  # Poll every 5 seconds
        
        # Timer for smooth time updates (every second)
        self.time_update_timer = QtCore.QTimer(self)
        self.time_update_timer.timeout.connect(self._update_running_jobs_time)
        self.time_update_timer.start(1000)  # Update every 1 second
        
        # Start event listener
        try:
            self._start_event_listener()
        except Exception as e:
            logger.warning("Failed to start event listener: %s", e)
    
    def _on_max_concurrent_changed(self, value):
        """Handler for changing maximum number of concurrent transfers."""
        try:
            # Save to settings
            self._settings.setValue("max_concurrent_transfers", value)
            
            # Update manager if available
            if MroyaTransferManagerWidget._global_manager:
                MroyaTransferManagerWidget._global_manager.set_max_concurrent_transfers(value)
                logger.info(f"Max concurrent transfers updated to {value}")
        except Exception as e:
            logger.warning(f"Error updating max concurrent transfers: {e}")
    
    def _on_max_workers_changed(self, value):
        """Handler for changing maximum number of threads for sequences."""
        try:
            # Save to settings
            self._settings.setValue("max_workers", value)
            logger.info(f"Max workers (threads per sequence) updated to {value}")
            # Note: this value will be applied to new jobs
        except Exception as e:
            logger.warning(f"Error updating max workers: {e}")
    
    def _start_event_listener(self):
        """Subscribe to ftrack transfer status events."""
        class _Bridge(QtCore.QObject):
            event_received = QtCore.Signal(dict)
            job_request_received = QtCore.Signal(str)  # job_id
        
        self._bridge = _Bridge()
        self._bridge.event_received.connect(self._on_transfer_event)
        self._bridge.job_request_received.connect(self._on_job_request)
        
        def _handler(event):
            """Handler for ftrack.transfer.status events - filters by hostname to prevent cross-machine jobs."""
            try:
                # IMPORTANT: Filter by hostname to prevent adding jobs from other machines to UI
                # This matches the logic in BackgroundTransferManager and _request_handler
                import socket
                current_hostname = socket.gethostname().lower()
                
                event_source = event.get('source', {})
                event_hostname = event_source.get('hostname')
                if event_hostname:
                    event_hostname = str(event_hostname).lower()
                
                data = event.get('data') or {}
                job_id = data.get('job_id')
                job_id_short = job_id[:8] if job_id else 'None'
                
                # Reject events without hostname or with mismatched hostname
                # This prevents jobs from other machines appearing in UI
                if not event_hostname:
                    event_user = event_source.get('user', {})
                    event_username = event_user.get('username') if isinstance(event_user, dict) else None
                    logger.debug(
                        "MroyaTransferManagerWidget: _handler REJECTED ftrack.transfer.status event without hostname "
                        "(job_id=%s, current_hostname=%s, event_user=%s, current_user=%s). "
                        "Events must have hostname for proper filtering.",
                        job_id_short,
                        current_hostname,
                        event_username or "UNKNOWN",
                        self.session.api_user
                    )
                    return
                
                if event_hostname != current_hostname:
                    event_user = event_source.get('user', {})
                    event_username = event_user.get('username') if isinstance(event_user, dict) else None
                    logger.debug(
                        "MroyaTransferManagerWidget: _handler REJECTED ftrack.transfer.status event from different hostname "
                        "(event_hostname=%s, current_hostname=%s, job_id=%s, event_user=%s, current_user=%s)",
                        event_hostname,
                        current_hostname,
                        job_id_short,
                        event_username or "UNKNOWN",
                        self.session.api_user
                    )
                    return
                
                # Hostname matches - pass event to UI
                self._bridge.event_received.emit(dict(data))
            except Exception as e:
                logger.warning("Event handler error: %s", e, exc_info=True)
        
        def _request_handler(event):
            """Handler for mroya.transfer.request event to immediately add job to UI."""
            try:
                # IMPORTANT: Check hostname match to prevent adding jobs from other machines
                # Even though subscription filter checks username, we need to verify hostname
                # because multiple users with same username on different machines shouldn't
                # interfere with each other.
                # 
                # NOTE: With our modified multi-site-location plugin, hostname is always set
                # when events are published from browser. Events without hostname are from
                # old versions and should be rejected for security (same logic as BackgroundTransferManager).
                import socket
                current_hostname = socket.gethostname().lower()
                
                event_source = event.get('source', {})
                event_hostname = event_source.get('hostname')
                if event_hostname:
                    event_hostname = str(event_hostname).lower()
                
                data = event.get('data') or {}
                job_id = data.get('job_id')
                job_id_short = job_id[:8] if job_id else 'None'
                
                # Reject events without hostname or with mismatched hostname
                # This matches the logic in BackgroundTransferManager._on_transfer_request
                event_user = event_source.get('user', {})
                event_username = event_user.get('username') if isinstance(event_user, dict) else None
                current_username = self.session.api_user
                
                if not event_hostname:
                    logger.warning(
                        "MroyaTransferManagerWidget: _request_handler REJECTED event without hostname "
                        "(job_id=%s, current_hostname=%s, event_user=%s, current_user=%s). "
                        "Events must have hostname for proper filtering.",
                        job_id_short,
                        current_hostname,
                        event_username or "UNKNOWN",
                        current_username
                    )
                    return
                
                if event_hostname != current_hostname:
                    logger.debug(
                        "MroyaTransferManagerWidget: _request_handler REJECTED event from different hostname "
                        "(event_hostname=%s, current_hostname=%s, job_id=%s, event_user=%s, current_user=%s)",
                        event_hostname,
                        current_hostname,
                        job_id_short,
                        event_username or "UNKNOWN",
                        current_username
                    )
                    return
                
                logger.info(
                    "MroyaTransferManagerWidget: _request_handler received event with job_id=%s, hostname=%s",
                    job_id_short,
                    event_hostname
                )
                if job_id:
                    self._bridge.job_request_received.emit(job_id)
                else:
                    logger.warning("MroyaTransferManagerWidget: _request_handler received event without job_id")
            except Exception as e:
                logger.warning("Job request handler error: %s", e, exc_info=True)
        
        try:
            self.session.event_hub.connect()
        except Exception:
            pass
        
        # Subscribe to transfer status events
        # NOTE: We subscribe to ALL ftrack.transfer.status events, but filter by hostname in _handler
        # to prevent adding jobs from other machines to UI
        self.session.event_hub.subscribe('topic=ftrack.transfer.status', _handler)
        # Subscribe to transfer requests for immediate display
        # Filter by current user only (same filter as BackgroundTransferManager)
        current_user = self.session.api_user
        request_topic_filter = (
            'topic=mroya.transfer.request and '
            'source.user.username="{0}"'
        ).format(current_user)
        self.session.event_hub.subscribe(request_topic_filter, _request_handler)
        
        def _run():
            try:
                while True:
                    self.session.event_hub.wait(1)
            except Exception:
                pass
        
        self._event_thread = threading.Thread(target=_run, daemon=True)
        self._event_thread.start()
    
    @QtCore.Slot(str)
    def _on_job_request(self, job_id):
        """Add job to UI immediately after receiving transfer request."""
        if job_id:
            logger.info(f"MroyaTransferManagerWidget: _on_job_request received job_id={job_id[:8]}, active_jobs count={len(self.active_jobs)}")
            self._add_job_from_id(job_id)
        else:
            logger.warning("MroyaTransferManagerWidget: _on_job_request received empty job_id")
    
    @QtCore.Slot(dict)
    def _on_transfer_event(self, data):
        """Update status row when we receive events."""
        try:
            job_id = data.get('job_id')
            status = data.get('status')
            if not job_id or not status:
                return
            
            job_info = self.active_jobs.get(job_id)
            if not job_info:
                # New job, add it
                logger.info(f"MroyaTransferManagerWidget: _on_transfer_event: job {job_id[:8]} not in active_jobs, adding it. active_jobs count={len(self.active_jobs)}")
                self._add_job_from_id(job_id)
                job_info = self.active_jobs.get(job_id)
                if not job_info:
                    logger.warning(f"MroyaTransferManagerWidget: _on_transfer_event: failed to add job {job_id[:8]}")
                    return
            
            row = job_info['row']
            status_item = self.job_table.item(row, 3)  # Status column
            progress_item = self.job_table.item(row, 4)  # Progress column
            time_item = self.job_table.item(row, 5)  # Time column
            speed_item = self.job_table.item(row, 6)  # Speed column
            
            if status_item:
                status_item.setText(status)
            
            progress = data.get('progress', 0.0)
            progress_percent = None
            if progress_item and progress is not None:
                progress_percent = int(progress * 100) if isinstance(progress, (int, float)) else 0
                progress_item.setText(f"{progress_percent}%")
            
            # Update speed from event
            # Time is now updated smoothly via timer, not via events
            speed_mbps = data.get('speed_mbps')
            
            if speed_mbps is not None:
                if speed_item:
                    speed_item.setText(f"{speed_mbps:.2f} MB/s")
                # Save last speed in job_info for smooth display
                if job_info:
                    job_info['last_speed'] = speed_mbps
            
            # Log UI update for debugging (duplicate what's shown in interface)
            elapsed_time = data.get('elapsed_time')
            transfer_logger = logging.getLogger('mroya_transfer')
            time_str = f"{elapsed_time:.1f}s" if elapsed_time is not None else "N/A"
            speed_str = f"{speed_mbps:.2f} MB/s" if speed_mbps is not None else "N/A"
            progress_str = f"{progress_percent}%" if progress_percent is not None else "N/A"
            transfer_logger.info(
                f"UI_UPDATE | job_id={job_id[:8]} | "
                f"status={status} | "
                f"progress={progress_str} ({progress:.4f}) | "
                f"time={time_str} | "
                f"speed={speed_str}"
            )
            
            # If time not in event, initialize start_time from job_data
            if 'start_time' not in job_info:
                try:
                    job = self.session.get('Job', job_id)
                    if job:
                        job_data = json.loads(job.get('data', '{}') or '{}')
                        start_time = job_data.get('start_time')
                        if start_time:
                            job_info['start_time'] = float(start_time)
                except Exception:
                    pass
            
            # Update row color based on status
            self._update_row_color(row, status)
            
            if status in ('done', 'failed'):
                self.active_jobs.pop(job_id, None)
        except Exception as e:
            logger.warning("Error handling transfer event: %s", e)
    
    def _format_time(self, seconds):
        """Format time into readable format."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = seconds % 60
            return f"{minutes}m {secs:.0f}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            secs = seconds % 60
            return f"{hours}h {minutes}m {secs:.0f}s"
    
    def _format_size(self, size_bytes):
        """Format size into readable format."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
    
    def _update_row_color(self, row, status):
        """Update row color based on job status."""
        # Determine color and text color based on status
        if status == 'running':
            # Blue for active task
            bg_color = QtGui.QColor(74, 144, 226)  # #4a90e2 in RGB
            text_color = QtGui.QColor(255, 255, 255)  # White text for contrast
        elif status == 'paused':
            # Yellow for paused
            bg_color = QtGui.QColor(255, 200, 0)  # Yellow
            text_color = QtGui.QColor(0, 0, 0)  # Black text for contrast
        else:
            # For all other statuses - don't set color (use table style)
            bg_color = None
            text_color = None
        
        # Apply colors to all cells in row (except Actions column, which has widget)
        for col in range(self.job_table.columnCount()):
            # Skip Actions column (index 7), it has widget, not item
            if col == 7:
                continue
                
            item = self.job_table.item(row, col)
            if item:
                if bg_color:
                    # Use setBackground directly with QColor (as in transfer_status_widget.py)
                    item.setBackground(bg_color)
                    # Also set via setData for reliability
                    item.setData(QtCore.Qt.ItemDataRole.BackgroundRole, bg_color)
                if text_color:
                    item.setForeground(text_color)
                    item.setData(QtCore.Qt.ItemDataRole.ForegroundRole, text_color)
    
    def _add_job_from_id(self, job_id):
        """Add a job to the table by fetching it from the server."""
        try:
            logger.info(f"MroyaTransferManagerWidget: _add_job_from_id called for job_id={job_id[:8] if job_id else 'None'}, active_jobs count={len(self.active_jobs)}, table rows={self.job_table.rowCount()}")
            # Check if job already added
            if job_id in self.active_jobs:
                logger.warning(f"MroyaTransferManagerWidget: job {job_id[:8]} already in active_jobs at row {self.active_jobs[job_id]['row']}, skipping duplicate add")
                return
            
            job = self.session.get('Job', job_id)
            if not job:
                logger.warning(f"MroyaTransferManagerWidget: job {job_id[:8]} not found in session")
                return
            
            job_data = json.loads(job.get('data', '{}') or '{}')
            tag = job_data.get('tag')
            logger.debug(f"MroyaTransferManagerWidget: job {job_id[:8]} tag={tag}, filter_tag={self._filter_tag}")
            if tag != self._filter_tag:
                logger.debug(f"MroyaTransferManagerWidget: job {job_id[:8]} tag mismatch, skipping")
                return
            
            # Don't add completed jobs
            status = job.get('status')
            if status in ('done', 'failed', 'killed'):
                return
            
            component_label = job_data.get('component_label', 'Unknown')
            to_location_name = job_data.get('to_location_name', 'Unknown')
            total_size_bytes = job_data.get('total_size_bytes', 0)
            
            row = self.job_table.rowCount()
            self.job_table.insertRow(row)
            
            self.job_table.setItem(row, 0, QtWidgets.QTableWidgetItem(component_label))
            self.job_table.setItem(row, 1, QtWidgets.QTableWidgetItem(to_location_name))
            
            # Size column
            size_text = self._format_size(total_size_bytes) if total_size_bytes > 0 else "N/A"
            self.job_table.setItem(row, 2, QtWidgets.QTableWidgetItem(size_text))
            
            # Status column
            self.job_table.setItem(row, 3, QtWidgets.QTableWidgetItem(job.get('status', 'unknown')))
            
            # Progress column
            progress = job_data.get('progress', 0.0)
            progress_percent = int(progress * 100) if isinstance(progress, (int, float)) else 0
            self.job_table.setItem(row, 4, QtWidgets.QTableWidgetItem(f"{progress_percent}%"))
            
            # Time column
            start_time = job_data.get('start_time')
            elapsed_time = job_data.get('elapsed_time', 0.0)
            if start_time:
                time_text = self._format_time(float(elapsed_time))
            else:
                time_text = "0s"
            self.job_table.setItem(row, 5, QtWidgets.QTableWidgetItem(time_text))
            
            # Speed column
            speed = job_data.get('speed_mbps')
            speed_text = f"{speed:.2f} MB/s" if speed is not None else "N/A"
            self.job_table.setItem(row, 6, QtWidgets.QTableWidgetItem(speed_text))
            
            # Actions column - control buttons with icons
            actions_widget = QtWidgets.QWidget()
            actions_layout = QtWidgets.QHBoxLayout(actions_widget)
            actions_layout.setContentsMargins(2, 2, 2, 2)
            actions_layout.setSpacing(3)
            
            # Get sizes accounting for DPI scaling (reduced for better placement)
            icon_size = _get_scaled_icon_size(12)  # Icon size 12px
            button_size_qsize = _get_scaled_button_size(20)  # Button size 20px (reduced)
            button_size = max(button_size_qsize.width(), button_size_qsize.height())  # Use maximum size
            
            # Button styles - make them more visible but compact
            button_style = """
                QPushButton {
                    background-color: #3a3a3a;
                    border: 1px solid #555555;
                    border-radius: 3px;
                    padding: 1px;
                    min-width: 18px;
                    min-height: 18px;
                    max-width: 20px;
                    max-height: 20px;
                }
                QPushButton:hover {
                    background-color: #4a4a4a;
                    border: 1px solid #666666;
                }
                QPushButton:pressed {
                    background-color: #2a2a2a;
                }
                QPushButton:disabled {
                    background-color: #2a2a2a;
                    border: 1px solid #333333;
                    opacity: 0.5;
                }
            """
            
            # Pause button with icon
            pause_button = QtWidgets.QPushButton()
            pause_button.setStyleSheet(button_style)
            pause_icon = _get_standard_icon(QtWidgets.QStyle.SP_MediaPause)
            if not pause_icon.isNull():
                pause_button.setIcon(pause_icon)
                pause_button.setIconSize(icon_size)
            else:
                pause_button.setText("||")  # Fallback: ASCII pause symbol
                pause_button.setStyleSheet(button_style + "font-size: 12px;")
            pause_button.setToolTip("Pause")
            pause_button.setEnabled(job.get('status') == 'running')
            pause_button.clicked.connect(partial(self._pause_job, job_id))
            pause_button.setFixedSize(button_size, button_size)
            actions_layout.addWidget(pause_button)
            
            # Resume button with icon
            resume_button = QtWidgets.QPushButton()
            resume_button.setStyleSheet(button_style)
            resume_icon = _get_standard_icon(QtWidgets.QStyle.SP_MediaPlay)
            if not resume_icon.isNull():
                resume_button.setIcon(resume_icon)
                resume_button.setIconSize(icon_size)
            else:
                resume_button.setText(">")  # Fallback: ASCII play symbol
                resume_button.setStyleSheet(button_style + "font-size: 12px;")
            resume_button.setToolTip("Resume")
            resume_button.setEnabled(job.get('status') == 'paused')
            resume_button.clicked.connect(partial(self._resume_job, job_id))
            resume_button.setFixedSize(button_size, button_size)
            actions_layout.addWidget(resume_button)
            
            # Stop button with icon
            stop_button = QtWidgets.QPushButton()
            stop_button.setStyleSheet(button_style)
            stop_icon = _get_standard_icon(QtWidgets.QStyle.SP_MediaStop)
            if not stop_icon.isNull():
                stop_button.setIcon(stop_icon)
                stop_button.setIconSize(icon_size)
            else:
                stop_button.setText("[]")  # Fallback: ASCII stop symbol
                stop_button.setStyleSheet(button_style + "font-size: 12px;")
            stop_button.setToolTip("Stop")
            stop_button.setEnabled(job.get('status') in ('running', 'paused', 'queued'))
            stop_button.clicked.connect(partial(self._stop_job, job_id))
            stop_button.setFixedSize(button_size, button_size)
            actions_layout.addWidget(stop_button)
            
            self.job_table.setCellWidget(row, 7, actions_widget)
            
            # Highlight row based on status
            self._update_row_color(row, job.get('status', 'unknown'))
            
            # Initialize job_info with start_time
            job_info = {'row': row}
            
            # Try to get start_time from job_data
            try:
                job_data = json.loads(job.get('data', '{}') or '{}')
                start_time = job_data.get('start_time')
                if start_time:
                    job_info['start_time'] = float(start_time)
                else:
                    # If start_time not available, use current time
                    import time
                    job_info['start_time'] = time.time()
            except Exception:
                import time
                job_info['start_time'] = time.time()
            
            job_info['last_elapsed_time'] = 0.0
            job_info['last_speed'] = None
            
            self.active_jobs[job_id] = job_info
            logger.info(f"MroyaTransferManagerWidget: job {job_id[:8]} added to UI at row {row}")
        except Exception as e:
            logger.warning("Error adding job: %s", e, exc_info=True)
    
    def _stop_job(self, job_id):
        """Stop transfer by setting Job status to 'killed'."""
        # Block all buttons immediately to prevent multiple clicks
        self._set_buttons_enabled(job_id, False)
        
        try:
            logger.info(f"Stop button clicked for job {job_id}")
            job = self.session.get('Job', job_id)
            if not job:
                logger.warning(f"Job {job_id} not found")
                # Re-enable buttons
                self._update_action_buttons(job_id, job.get('status') if job else 'unknown')
                return
            
            current_status = job.get('status')
            logger.info(f"Job {job_id} current status: {current_status}")
            
            # Check if can be stopped
            if current_status not in ('running', 'paused', 'queued'):
                logger.info(f"Job {job_id} cannot be stopped (status: {current_status})")
                # Re-enable buttons
                self._update_action_buttons(job_id, current_status)
                return
            
            # Set status to 'killed'
            job['status'] = 'killed'
            try:
                job_data = json.loads(job.get('data', '{}') or '{}')
                job_data['description'] = 'Transfer cancelled by user'
                job['data'] = json.dumps(job_data)
                self.session.commit()
                
                # Log stop
                logger.info(f"Job {job_id} status set to 'killed' and committed")
                transfer_logger.info(f"JOB_STOPPED | job_id={job_id} | by user")
                
                # Update buttons in table (they will remain disabled for killed status)
                self._update_action_buttons(job_id, 'killed')
            except Exception as e:
                logger.error(f"Error stopping job {job_id}: {e}", exc_info=True)
                # On error, re-enable buttons
                self._update_action_buttons(job_id, current_status)
        except Exception as e:
            logger.error(f"Error in _stop_job for {job_id}: {e}", exc_info=True)
            # On error, try to re-enable buttons
            try:
                job = self.session.get('Job', job_id)
                if job:
                    self._update_action_buttons(job_id, job.get('status'))
            except Exception:
                pass
    
    def _pause_job(self, job_id):
        """Pause transfer by setting Job status to 'paused'."""
        # Block all buttons immediately to prevent multiple clicks
        self._set_buttons_enabled(job_id, False)
        
        try:
            logger.info(f"Pause button clicked for job {job_id}")
            job = self.session.get('Job', job_id)
            if not job:
                # Re-enable buttons
                self._update_action_buttons(job_id, 'unknown')
                return
            
            current_status = job.get('status')
            if current_status != 'running':
                # Re-enable buttons
                self._update_action_buttons(job_id, current_status)
                return
            
            # Set status to 'paused'
            job['status'] = 'paused'
            try:
                job_data = json.loads(job.get('data', '{}') or '{}')
                job_data['description'] = 'Transfer paused by user'
                job['data'] = json.dumps(job_data)
                self.session.commit()
                
                transfer_logger.info(f"JOB_PAUSED | job_id={job_id} | by user")
                
                # Update buttons in table
                self._update_action_buttons(job_id, 'paused')
            except Exception as e:
                logger.warning(f"Error pausing job {job_id}: {e}")
                # On error, re-enable buttons
                self._update_action_buttons(job_id, current_status)
        except Exception as e:
            logger.warning(f"Error in _pause_job for {job_id}: {e}")
            # On error, try to re-enable buttons
            try:
                job = self.session.get('Job', job_id)
                if job:
                    self._update_action_buttons(job_id, job.get('status'))
            except Exception:
                pass
    
    def _resume_job(self, job_id):
        """Resume transfer by setting Job status to 'queued' for reprocessing."""
        # Block all buttons immediately to prevent multiple clicks
        self._set_buttons_enabled(job_id, False)
        
        try:
            logger.info(f"Resume button clicked for job {job_id}")
            job = self.session.get('Job', job_id)
            if not job:
                # Re-enable buttons
                self._update_action_buttons(job_id, 'unknown')
                return
            
            current_status = job.get('status')
            if current_status != 'paused':
                # Re-enable buttons
                self._update_action_buttons(job_id, current_status)
                return
            
            # Set status to 'queued' for reprocessing
            job['status'] = 'queued'
            try:
                job_data = json.loads(job.get('data', '{}') or '{}')
                job_data['description'] = 'Transfer resumed by user'
                job['data'] = json.dumps(job_data)
                self.session.commit()
                
                transfer_logger.info(f"JOB_RESUMED | job_id={job_id} | by user")
                
                # Update buttons in table
                self._update_action_buttons(job_id, 'queued')
                
                # Publish event for reprocessing
                try:
                    self.session.event_hub.publish(
                        ftrack_api.event.base.Event(
                            topic='mroya.transfer.request',
                            data={
                                'job_id': job_id,
                                'user_id': job.get('user_id'),
                                'from_location_id': job_data.get('from_location_id'),
                                'to_location_id': job_data.get('to_location_id'),
                                'selection': job_data.get('selection', []),
                            }
                        ),
                        on_error='ignore'
                    )
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"Error resuming job {job_id}: {e}")
                # On error, re-enable buttons
                self._update_action_buttons(job_id, current_status)
        except Exception as e:
            logger.warning(f"Error in _resume_job for {job_id}: {e}")
            # On error, try to re-enable buttons
            try:
                job = self.session.get('Job', job_id)
                if job:
                    self._update_action_buttons(job_id, job.get('status'))
            except Exception:
                pass
    
    def _get_action_buttons(self, job_id):
        """Get control buttons for job."""
        job_info = self.active_jobs.get(job_id)
        if not job_info:
            return None, None, None
        
        row = job_info['row']
        actions_widget = self.job_table.cellWidget(row, 7)
        if not actions_widget:
            return None, None, None
        
        pause_button = None
        resume_button = None
        stop_button = None
        
        for i in range(actions_widget.layout().count()):
            widget = actions_widget.layout().itemAt(i).widget()
            if isinstance(widget, QtWidgets.QPushButton):
                tooltip = widget.toolTip()
                if tooltip == "Pause":
                    pause_button = widget
                elif tooltip == "Resume":
                    resume_button = widget
                elif tooltip == "Stop":
                    stop_button = widget
        
        return pause_button, resume_button, stop_button
    
    def _set_buttons_enabled(self, job_id, enabled):
        """Enable/disable all buttons for job."""
        pause_button, resume_button, stop_button = self._get_action_buttons(job_id)
        if pause_button:
            pause_button.setEnabled(enabled)
        if resume_button:
            resume_button.setEnabled(enabled)
        if stop_button:
            stop_button.setEnabled(enabled)
        # Update UI immediately
        QtWidgets.QApplication.processEvents()
    
    def _update_action_buttons(self, job_id, status):
        """Update control button state for job."""
        try:
            pause_button, resume_button, stop_button = self._get_action_buttons(job_id)
            if not pause_button and not resume_button and not stop_button:
                return
            
            if pause_button:
                pause_button.setEnabled(status == 'running')
            if resume_button:
                resume_button.setEnabled(status == 'paused')
            if stop_button:
                stop_button.setEnabled(status in ('running', 'paused', 'queued'))
            
            # Update status in table
            job_info = self.active_jobs.get(job_id)
            if job_info:
                row = job_info['row']
                status_item = self.job_table.item(row, 3)
                if status_item:
                    status_item.setText(status)
                
                # Update row color
                self._update_row_color(row, status)
        except Exception as e:
            logger.warning(f"Error updating action buttons for job {job_id}: {e}")
    
    @QtCore.Slot()
    def _update_running_jobs_time(self):
        """Smoothly update time for all running jobs every second."""
        import time
        current_time = time.time()
        
        for job_id, job_info in list(self.active_jobs.items()):
            try:
                row = job_info.get('row')
                if row is None:
                    continue
                
                # Get status from table
                status_item = self.job_table.item(row, 3)
                if not status_item:
                    continue
                
                status = status_item.text()
                
                # Update time only for running jobs
                if status == 'running':
                    start_time = job_info.get('start_time')
                    if start_time:
                        elapsed_time = current_time - start_time
                        
                        # Update time in table
                        time_item = self.job_table.item(row, 5)
                        if time_item:
                            time_item.setText(self._format_time(elapsed_time))
                        
                        # Save for later use
                        job_info['last_elapsed_time'] = elapsed_time
                        
                        # Log time update via timer (less frequently to avoid log spam - every 5 seconds)
                        if not hasattr(self, '_last_time_log'):
                            self._last_time_log = {}
                        last_log_time = self._last_time_log.get(job_id, 0)
                        if current_time - last_log_time >= 5.0:
                            transfer_logger = logging.getLogger('mroya_transfer')
                            # Get current values from UI
                            progress_item = self.job_table.item(row, 4)
                            speed_item = self.job_table.item(row, 6)
                            progress_text = progress_item.text() if progress_item else "N/A"
                            speed_text = speed_item.text() if speed_item else "N/A"
                            transfer_logger.info(
                                f"UI_TIMER_UPDATE | job_id={job_id[:8]} | "
                                f"status={status} | "
                                f"progress={progress_text} | "
                                f"time={self._format_time(elapsed_time)} | "
                                f"speed={speed_text}"
                            )
                            self._last_time_log[job_id] = current_time
            except Exception as e:
                logger.warning(f"Error updating time for job {job_id[:8] if job_id else '?'}: {e}")
    
    def _check_job_statuses(self):
        """Periodically check the status of active jobs."""
        if not self.active_jobs:
            return
        
        try:
            job_ids = list(self.active_jobs.keys())
            jobs = self.session.query('Job where id in ({})'.format(','.join(f'"{jid}"' for jid in job_ids))).all()
            
            for job in jobs:
                job_id = job['id']
                job_info = self.active_jobs.get(job_id)
                if not job_info:
                    continue
                
                row = job_info['row']
                status_item = self.job_table.item(row, 3)  # Status column
                progress_item = self.job_table.item(row, 4)  # Progress column
                time_item = self.job_table.item(row, 5)  # Time column
                speed_item = self.job_table.item(row, 6)  # Speed column
                
                if status_item:
                    status_item.setText(job.get('status', 'unknown'))
                
                # Progress updates from events, not from job_data
                # This prevents conflicts and value jumps due to asynchronous job_data updates
                # If progress not set in UI (first time), set from job_data as fallback
                if progress_item and not progress_item.text():
                    job_data = json.loads(job.get('data', '{}') or '{}')
                    progress = job_data.get('progress', 0.0)
                    if progress is not None:
                        progress_percent = int(progress * 100) if isinstance(progress, (int, float)) else 0
                        progress_item.setText(f"{progress_percent}%")
                
                # Time and speed update from events, not from job_data
                # This prevents conflicts and value jumps due to asynchronous job_data updates
                # If time/speed not set in UI (first time), set from job_data as fallback
                if time_item and not time_item.text():
                    # Only if time hasn't been set from event yet
                    start_time = job_data.get('start_time')
                    elapsed_time = job_data.get('elapsed_time')
                    if start_time and elapsed_time is not None:
                        elapsed_seconds = float(elapsed_time)
                        time_item.setText(self._format_time(elapsed_seconds))
                    else:
                        time_item.setText("0s")
                
                if speed_item and not speed_item.text():
                    # Only if speed hasn't been set from event yet
                    speed = job_data.get('speed_mbps')
                    if speed is not None:
                        speed_item.setText(f"{speed:.2f} MB/s")
                    else:
                        speed_item.setText("N/A")
                
                status = job.get('status')
                # Update row color based on status
                self._update_row_color(row, status)
                
                # Update control buttons
                self._update_action_buttons(job_id, status)
                
                if status in ('done', 'failed'):
                    self.active_jobs.pop(job_id, None)
        except Exception as e:
            logger.warning("Error checking job statuses: %s", e)


# --------------------------------------------------------------------------- 
# Plugin registration
# --------------------------------------------------------------------------- 

def register(session: ftrack_api.Session, **kw) -> None:  # type: ignore[name-defined]
    """Plugin registration."""
    logger.info("=" * 80)
    logger.info("MroyaTransferManager: register() called")
    logger.info("=" * 80)
    
    # Validate that session is an instance of ftrack_api.Session
    if not isinstance(session, ftrack_api.session.Session):
        logger.warning(
            'Not registering plugin as passed argument {0!r} is not an '
            'ftrack_api.Session instance.'.format(session)
        )
        return
    
    logger.info("MroyaTransferManager: Session validation passed")
    
    # Check that custom transfer is available
    if transfer_component_custom is None:
        error_msg = (
            'MroyaTransferManager: transfer_component_custom not available. '
            'Cannot register plugin. Import error: {0}'.format(_import_error)
        )
        logger.error(error_msg)
        transfer_logger.error(error_msg)
        raise ImportError(error_msg)
    
    logger.info("MroyaTransferManager: transfer_component_custom is available")
    
    # Create and register background manager.
    # Load max_concurrent_transfers setting from QSettings
    try:
        from ftrack_connect.qt import QtCore  # pyright: ignore[reportMissingImports]
        settings = QtCore.QSettings("mroya", "TransferManager")
        max_concurrent = settings.value("max_concurrent_transfers", 1, type=int)
        logger.info("MroyaTransferManager: max_concurrent_transfers=%d", max_concurrent)
    except Exception as e:
        logger.warning("MroyaTransferManager: Failed to load settings: %s", e)
        max_concurrent = 1
    
    logger.info("MroyaTransferManager: Creating BackgroundTransferManager...")
    try:
        manager = BackgroundTransferManager(session, max_concurrent_transfers=max_concurrent)
        logger.info("MroyaTransferManager: BackgroundTransferManager created successfully")
    except Exception as e:
        logger.error("MroyaTransferManager: Failed to create BackgroundTransferManager: %s", e, exc_info=True)
        transfer_logger.error("MroyaTransferManager: Failed to create BackgroundTransferManager: %s", e, exc_info=True)
        raise
    
    logger.info("MroyaTransferManager: Calling manager.register()...")
    try:
        manager.register()
        logger.info("MroyaTransferManager: manager.register() completed successfully")
    except Exception as e:
        logger.error("MroyaTransferManager: Failed to register manager: %s", e, exc_info=True)
        transfer_logger.error("MroyaTransferManager: Failed to register manager: %s", e, exc_info=True)
        raise
    
    # Save reference to manager for access from widget
    MroyaTransferManagerWidget._global_manager = manager
    logger.info("MroyaTransferManager: Manager reference saved to widget")
    
    # Register widget in Connect UI.
    logger.info("MroyaTransferManager: Registering widget in Connect UI...")
    try:
        plugin = ftrack_connect.ui.application.ConnectWidgetPlugin(MroyaTransferManagerWidget)
        plugin.register(session, priority=50)
        logger.info('MroyaTransferManager plugin registered successfully')
        transfer_logger.info('MroyaTransferManager plugin registered successfully')
    except Exception as e:
        logger.error("MroyaTransferManager: Failed to register widget: %s", e, exc_info=True)
        transfer_logger.error("MroyaTransferManager: Failed to register widget: %s", e, exc_info=True)
        raise
