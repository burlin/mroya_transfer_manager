# Mroya Transfer Manager

Ftrack Connect plugin for managing background component transfers between locations.

## Description

Plugin provides:
- **Background transfer manager** - processes component transfer requests via `mroya.transfer.request` events
- **UI widget in ftrack Connect** - displays transfer status and allows process management
- **Integration with TransferStatusDialog** - uses shared status widget with browser and UserTasksWidget

## Installation

1. Add plugin path to `FTRACK_CONNECT_PLUGIN_PATH`:
   ```
   FTRACK_CONNECT_PLUGIN_PATH=C:\path\to\ftrack_plugins
   ```

2. Ensure dependencies are installed:
   - `ftrack_inout` - for `transfer_status_widget`
   - `boto3` - for S3 multipart upload (usually available in `multi-site-location-0.2.0/dependencies`)
   - `fileseq` - for sequence file handling

3. Restart ftrack Connect

## Dependencies

Plugin depends on:
- `ftrack_inout.browser.transfer_status_widget` - transfer status widget
- `boto3` - for S3 operations
- `fileseq` - for sequence file processing

## Usage

Plugin automatically registers on ftrack Connect startup and:
1. Subscribes to `mroya.transfer.request` events
2. Runs transfers in background threads
3. Publishes `ftrack.transfer.status` events for UI updates
4. Adds "Transfer Manager" tab in ftrack Connect

## Settings

Settings stored in QSettings (`mroya/TransferManager`):
- `max_concurrent_transfers` - maximum number of concurrent transfers (default: 1)

## Version

0.1.0
