# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Splunk Add-on** built using the **UCC (Universal Configuration Console) Framework** for collecting Microsoft Teams Auto Attendant and Call Queue reporting data. The add-on is in early development (v0.0.1).

**Official UCC Documentation**: https://splunk.github.io/addonfactory-ucc-generator/

## Build and Development Workflow

### Building the Add-on

The UCC framework uses `globalConfig.json` as the source of truth to generate the Splunk app structure. To build:

```bash
ucc-gen build
```

This command:
- Reads `globalConfig.json`
- Generates the complete Splunk app structure in the `output/` directory
- Creates the web UI components, configuration handlers, and REST endpoints
- Packages everything into a `.spl` file (Splunk app package)

### Installing Dependencies

The add-on requires Python libraries listed in `package/lib/requirements.txt`:
```
splunktaucclib
splunk-sdk
solnlib
```

Dependencies are bundled into the `package/lib/` directory during the build process.

## Architecture

### Configuration-Driven Design

This add-on uses **declarative configuration** via `globalConfig.json` rather than traditional imperative code. The UCC framework generates most components automatically.

**Key Configuration Sections:**

1. **Account Configuration** (`pages.configuration.tabs[0]`)
   - Stores encrypted API credentials
   - Fields: `name`, `api_key` (encrypted)
   - Stored in: `conf-splunk_msteams_aa_callqueue_reporting_addon_account`

2. **Input Configuration** (`pages.inputs.services[0]`)
   - Name: `input`
   - References the modular input: `input_helper.py`
   - Fields: `name`, `interval` (10-301 seconds, default 300), `index`, `account`

3. **Logging Tab** (`pages.configuration.tabs[1]`)
   - Auto-generated logging configuration UI
   - Controls log levels via `conf-splunk_msteams_aa_callqueue_reporting_addon_settings`

### Data Collection Flow

```
User configures input in Splunk UI
         ↓
globalConfig.json defines UI schema
         ↓
Splunk scheduler triggers modular input based on interval
         ↓
input_helper.py::stream_events() is invoked
         ↓
Retrieves encrypted API key from account config
         ↓
Calls get_data_from_api() [CURRENTLY RETURNS DUMMY DATA]
         ↓
Writes events to Splunk with sourcetype "dummy-data"
```

### Modular Input Implementation

**File**: `package/bin/input_helper.py`

**Entry Points:**
- `validate_input(definition)` - Input validation (currently a no-op)
- `stream_events(inputs, event_writer)` - Main data collection logic

**Key Functions:**
- `logger_for_input(input_name)` - Creates input-specific logger
- `get_account_api_key(session_key, account_name)` - Retrieves encrypted API key using solnlib's ConfManager
- `get_data_from_api(logger, api_key)` - **[TO BE IMPLEMENTED]** Currently returns dummy data

**Configuration Access Pattern:**
```python
# Encrypted credentials are stored in Splunk's secure storage
cfm = conf_manager.ConfManager(
    session_key,
    ADDON_NAME,
    realm=f"__REST_CREDENTIAL__#{ADDON_NAME}#configs/conf-splunk_msteams_aa_callqueue_reporting_addon_account",
)
account_conf_file = cfm.get_conf("splunk_msteams_aa_callqueue_reporting_addon_account")
api_key = account_conf_file.get(account_name).get("api_key")
```

### Missing Required File

The modular input imports `import_declare_test` which **does not exist yet**. This file is required by UCC to set up Python path for dependencies in `lib/`.

**Standard import_declare_test.py structure:**
```python
import os
import sys
import re

ta_name = 'splunk_msteams_aa_callqueue_reporting_addon'
pattern = re.compile(r'[\\/]etc[\\/]apps[\\/][^\\/]+[\\/]bin[\\/]?$')
new_paths = [path for path in sys.path if not pattern.search(path) or ta_name in path]
new_paths.insert(0, os.path.sep.join([os.path.dirname(__file__), '..', 'lib']))
sys.path = new_paths
```

This should be created at: `package/bin/import_declare_test.py`

## Important Constants

- **Add-on Name**: `splunk_msteams_aa_callqueue_reporting_addon`
- **Display Name**: MSTeams Auto Attendant and Call Queue Reporting Add-on for Splunk
- **REST Root**: `splunk_msteams_aa_callqueue_reporting_addon`
- **Current Sourcetype**: `dummy-data` (hardcoded in input_helper.py:69)
- **Schema Version**: 0.0.8 (UCC schema)

## Deployment Support

The add-on supports:
- Standalone deployments
- Distributed deployments
- Search Head Clustering

Target workloads: Search Heads and Indexers

## Modifying the Add-on

### Adding New Input Fields

1. Edit `globalConfig.json` → `pages.inputs.services[0].entity`
2. Add field definition with validators
3. Rebuild with `ucc-gen build`
4. Access in `input_helper.py` via `input_item.get("field_name")`

### Changing Account Configuration

1. Edit `globalConfig.json` → `pages.configuration.tabs[0].entity`
2. For encrypted fields, set `"encrypted": true`
3. Rebuild with `ucc-gen build`

### Implementing Real Data Collection

The current `get_data_from_api()` function in `input_helper.py:25-35` returns dummy data. Replace this with actual Microsoft Teams Graph API calls to collect Auto Attendant and Call Queue analytics.

**Key considerations:**
- Use the `api_key` parameter for authentication (may need to be changed to OAuth tokens)
- Return data as list of dictionaries for JSON serialization
- Update `sourcetype` from "dummy-data" to appropriate value (e.g., "msteams:callqueue" or "msteams:autoattendant")
- Handle pagination if API returns large datasets
- Implement proper error handling and logging

## File Structure Context

```
package/
├── bin/                          # Executables and modular inputs
│   ├── input_helper.py          # Main data collection logic
│   └── import_declare_test.py   # [MISSING] Python path setup
├── lib/                          # Python dependencies
│   ├── requirements.txt         # solnlib, splunktaucclib, splunk-sdk
│   └── exclude.txt              # Files to exclude from packaging
└── static/                       # UI assets (icons)
```

The `globalConfig.json` at the root drives **all** UI generation, REST endpoints, and configuration schemas. Do not manually create configuration files in `default/` or `local/` - they are auto-generated by UCC.
