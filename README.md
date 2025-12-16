# Splunk Add-on for MS Teams Auto Attendant and Call Queue Reporting

A Splunk modular input add-on for collecting Microsoft Teams Voice Analytics and Aggregation Capability (VAAC) API data, with comprehensive enrichment and Power Query-compatible transformations.

## Overview

This add-on enables Splunk to collect and enrich call analytics data from Microsoft Teams Auto Attendants and Call Queues. It authenticates via OAuth, queries the VAAC API, transforms ordered array responses to structured events, and ingests enriched data into Splunk for analysis and reporting.

## Features

- **VAAC API Integration**
  - OAuth password grant flow authentication
  - Automatic ordered array-to-dictionary transformation
  - Configurable query dimensions and measurements
  - Support for both Call Queue and Auto Attendant reporting

- **Intelligent Data Collection**
  - Checkpoint-based incremental collection using `UserStartTimeUTC` filtering
  - Prevents duplicate and missing events
  - Configurable lookback intervals
  - Automatic retry and error handling

- **Comprehensive Enrichment**
  - 40+ calculated fields per record
  - Timezone conversion with automatic DST handling
  - Legend code mapping (4000-series codes for Call Queue)
  - Power Query-compatible transformations
  - Parallel processing for performance

- **Enterprise Ready**
  - Built on Splunk's UCC Framework
  - Encrypted credential storage
  - Configurable log levels
  - Support for distributed deployments

## Architecture

```
User Configuration (Splunk UI)
         ↓
   Modular Input (input_helper.py)
         ↓
   OAuth Authentication
         ↓
   VAAC API Query (compressed & encoded)
         ↓
   Ordered Array Response
         ↓
   Transform to Dictionaries
         ↓
   Enrichment (callqueue_enrichment.py / autoattendant_enrichment.py)
         ↓
   Splunk Event Writer
         ↓
   Index (sourcetype: msteams:vaac:callqueue or msteams:vaac:autoattendant)
```

## Prerequisites

### Splunk Environment
- Splunk Enterprise 8.0+ or Splunk Cloud
- Python 3.7+ (included with Splunk)

### Microsoft 365 Requirements
- Microsoft 365 tenant with Teams Phone System
- Teams Call Queue and/or Auto Attendant configured
- VAAC API access credentials (ROPC):
  - User email address
  - User password
  - Tenant ID

*note only ROPC authentication is supported by Microsoft (confirmed by their PM's)

### Python Dependencies
The following dependencies are bundled in `package/lib/`:
- `splunktaucclib` - UCC framework library
- `splunk-sdk` - Splunk SDK for Python
- `solnlib` - Splunk solution library
- `requests` - HTTP library

## Installation

### Option 1: Build from Source

```bash
# Install UCC generator
pip install splunk-add-on-ucc-framework

# Navigate to project directory
cd /path/to/splunk-msteams-aa-callqueue-reporting

# Build the add-on
ucc-gen build

# The packaged add-on will be in output/ directory
```

### Option 2: Install Pre-built Package

1. Copy the `.spl` file to your Splunk server
2. Install via Splunk Web: **Apps** > **Manage Apps** > **Install app from file**
3. Restart Splunk if prompted

## Configuration

### Step 1: Add Account Credentials

1. Navigate to **MSTeams AA/CQ Reporting Add-on** > **Configuration** > **Account**
2. Click **Add**
3. Configure:
   - **Account Name**: Descriptive name (e.g., "Production VAAC API")
   - **Email**: Microsoft 365 user email with VAAC API access
   - **Password**: User password (encrypted in Splunk storage)
   - **Tenant ID**: Your Microsoft 365 tenant ID (GUID format)
4. Click **Save**

### Step 2: Create Data Input

1. Navigate to **Inputs** tab
2. Click **Create New Input**
3. Configure:
   - **Name**: Unique input name (e.g., "callqueue_hourly")
   - **Interval**: Collection frequency in seconds (minimum: 300)
   - **Index**: Target Splunk index
   - **Account**: Select account from Step 1
   - **Report Type**: `call_queue` or `auto_attendant`
   - **Timezone**: Target timezone for local time conversion (e.g., "Australia/Sydney")
   - **Parallel Workers**: Number of threads for enrichment (default: 4)
   - **Limit Result Rows**: Max rows per API call (default: 200000)
4. Click **Save**

## Data Collection Details

### VAAC API Query Structure

The add-on constructs queries with the following components:

**Filters:**
- `UserStartTimeUTC >= <checkpoint_datetime>` - Precise time filtering
- `Date >= <start_date>` - Day boundary (start)
- `Date <= <end_date>` - Day boundary (end)

**Dimensions (Common):**
- DocumentId, ConferenceId, DialogId - Call identifiers
- UserStartTimeUTC, EndTime, Date - Timestamp fields

**Dimensions (Call Queue Specific):**
- CallQueueIdentity, CallQueueId
- CallQueueCallResult, CallQueueTargetType
- CallQueueDurationSeconds, CallQueueAgentCount, CallQueueAgentOptInCount
- And more...

**Measurements:**
- PSTNTotalMinutes
- TotalCallCount
- AvgCallDuration (optional)
- AvgCallQueueDurationSeconds (optional)

### Ordered Array Transformation

The VAAC API returns data as ordered arrays:
```json
{
  "dataResult": [
    ["2025-12-15T23:59:41", true, "agent_joined_conference", "CQ@example.com", 1, 3.0, 0.05],
    ...
  ]
}
```

The add-on automatically transforms these to dictionaries:
```json
{
  "UserStartTimeUTC": "2025-12-15T23:59:41",
  "HasCQ": true,
  "CallQueueCallResult": "agent_joined_conference",
  "CallQueueIdentity": "CQ@example.com",
  "TotalCallCount": 1,
  "AvgCallDuration": 3.0,
  "TotalAudioStreamDuration": 0.05
}
```

### Checkpoint Management

Checkpoints are stored in Splunk KVStore:
- **Collection**: `splunk_msteams_checkpoints`
- **Key Format**: `{input_name}_last_processed`
- **Stored Data**:
  - `last_datetime`: ISO format datetime of last successful run
  - `processed_records`: Count of records processed
  - `updated_at`: Timestamp of checkpoint update
  - `report_type`: Type of report (call_queue/auto_attendant)

On first run, the add-on uses interval-based lookback. Subsequent runs use the checkpoint datetime for incremental collection.

## Enrichment Reference

### Call Queue Enriched Fields

The add-on adds 40+ enriched fields to each Call Queue record. This enrichment utilises the logic from VAAC API in PowerBI M code PowerQuery, which is converted to Python. 

**Identifiers:**
- `CallQueue[DocumentId]`, `CallQueue[ConferenceId]`, `CallQueue[DialogId]`
- `CallQueue[CQGUID]` - Call Queue GUID

**Timestamps:**
- `CallQueue[CallStartTimeUTC]`, `CallQueue[CallEndTimeUTC]` - Parsed UTC timestamps
- `CallQueue[CallStartTimeLocal]`, `CallQueue[CallEndTimeLocal]` - Timezone-converted
- `CallQueue[CallStartDateLocal]` - Date component (local time)
- `CallQueue[Date]` - Hourly timestamp for aggregation
- `CallQueue[CQHour]` - Hour of day (0-23)

**Queue Information:**
- `CallQueue[CQRAName]` - Resource account name (extracted from identity)
- `CallQueue[CQSlicer]` - Slicer field for visualizations
- `CallQueue[CQName]` - Friendly queue name (if configured)
- `CallQueue[CQAgentCount]`, `CallQueue[CQAgentOptInCount]`

**Call Outcome:**
- `CallQueue[CQTargetType]` - Corrected target type
- `CallQueue[CQCallResultRaw]` - Raw call result from API
- `CallQueue[CQCallResultLegendCode]` - High-level result code (4001-4005, 4999)
- `CallQueue[CQTargetTypeLegendCode]` - Detailed disposition code (4010-4034)
- `CallQueue[CQCallResultLegendString]`, `CallQueue[CQTargetTypeLegendString]`

**Metrics:**
- `CallQueue[CQCallCount]` - Total call count
- `CallQueue[CQCallCountAbandoned]` - Abandoned flag (0 or 1)
- `CallQueue[CQCallDurationSeconds]` - Duration in queue
- `CallQueue[PSTNTotalMinutes]` - PSTN minutes

**Connectivity:**
- `CallQueue[CQConnectivityTypeCode]` - Numeric code (8600-8620)
- `CallQueue[CQConnectivityTypeString]` - Human-readable (e.g., "Calling Plan")
- `CallQueue[CQConnectivityTypeRaw]` - Raw value from API

**Other:**
- `CallQueue[DateTimeCQName]` - Composite key for deduplication
- `CallQueue[LanguageCode]` - Language code for localization

### Legend Codes Reference

**Call Result Legend Codes (High-Level):**
- `4001` - Agent Answered
- `4002` - Overflowed
- `4003` - Timed Out
- `4004` - No Agents
- `4005` - Other
- `4999` - Not Authorized

**Target Type Legend Codes (Detailed Disposition):**
- `4010` - Agent Answered (Call)
- `4011` - Agent Answered (Callback)
- `4012` - Abandoned
- `4013` - Overflowed (Application)
- `4014` - Overflowed (Voicemail)
- `4015` - Overflowed (Disconnect)
- `4016` - Overflowed (External)
- `4017` - Overflowed (User)
- `4020` - Timed Out (Application)
- `4021` - Timed Out (Voicemail)
- `4022` - Timed Out (Disconnect)
- `4023` - Timed Out (External)
- `4024` - Timed Out (User)
- `4025` - Timed Out (Callback)
- `4030` - No Agents (Application)
- `4031` - No Agents (Voicemail)
- `4032` - No Agents (Disconnect)
- `4033` - No Agents (External)
- `4034` - No Agents (User)
- `4005` - Other

**Connectivity Type Codes:**
- `8600` - Calling Plan
- `8601` - Direct Routing
- `8602` - Operator Connect
- `8610` - ACS Call
- `8620` - Unknown/Blank

## File Structure

```
splunk-msteams-aa-callqueue-reporting/
├── README.md                           # This file
├── CLAUDE.md                          # AI assistant guidance
├── globalConfig.json                  # UCC framework configuration
├── response_json_example.json         # Sample VAAC API response
├── splunk_output_expected.json        # Expected enriched output format
├── common_dimensions.txt              # Common dimension reference
├── powerquery.txt                     # Power Query reference logic
└── splunk_msteams_aa_callqueue_reporting_addon/
    └── package/
        ├── bin/
        │   ├── input_helper.py        # Main modular input
        │   ├── dimension_config.py    # Dimension/measurement configuration
        │   ├── callqueue_enrichment.py    # Call Queue enrichment logic
        │   ├── autoattendant_enrichment.py # Auto Attendant enrichment logic
        │   └── import_declare_test.py     # Python path setup
        └── lib/
            └── requirements.txt       # Python dependencies
```

## Development

### Building the Add-on

```bash
# Install development dependencies
pip install splunk-add-on-ucc-framework

# Build
ucc-gen build

# Output will be in output/ directory
```

### Testing

1. **Local Testing**: Use the standalone test script:
   ```bash
   python ms_vaac_quick_script.py
   ```
   This script demonstrates the VAAC API query construction and ordered array transformation.

2. **Splunk Testing**:
   - Install the built add-on on a test Splunk instance
   - Configure with valid credentials
   - Create a test input with short interval (e.g., 300 seconds)
   - Monitor logs: `index=_internal source=*msteams*`
   - Verify data: `index=<your_index> sourcetype=msteams:vaac:callqueue`

### Modifying Dimensions

To add/remove dimensions:

1. Edit `package/bin/dimension_config.py`
2. Update `CALL_QUEUE_DIMENSIONS` or `AUTO_ATTENDANT_DIMENSIONS` lists
3. **Important**: Common dimensions must remain first in the list
4. Rebuild with `ucc-gen build`

### AI Assistant Guidance

For detailed guidance on working with this codebase using Claude Code or other AI assistants, see `CLAUDE.md`.

## Troubleshooting

### Common Issues

**Issue: No data collected**
- Check credentials are valid and account has VAAC API access
- Verify tenant ID is correct (GUID format)
- Check Splunk logs: `index=_internal source=*msteams* ERROR`

**Issue: "NOTAUTHCQ" in results**
- User account doesn't have authorization to view Call Queue data
- Check Microsoft 365 admin roles and permissions

**Issue: Duplicate events**
- Verify checkpoint is being updated (check KVStore collection)
- Ensure `UserStartTimeUTC` dimension is in dimension config
- Check for multiple inputs with same name

**Issue: Missing timestamps**
- Verify timezone configuration is valid (e.g., "Australia/Sydney")
- Check `UserStartTimeUTC` is present in API response

### Log Locations

- **Add-on Logs**: `index=_internal source=*msteams*`
- **Modular Input Logs**: `index=_internal sourcetype=splunkd component=ExecProcessor msteams`
- **UCC Framework Logs**: `index=_internal sourcetype=splunkd ucc`

### Debug Mode

To enable debug logging:
1. Navigate to **Configuration** > **Logging**
2. Set **Log level** to `DEBUG`
3. Click **Save**
4. Monitor logs for detailed execution information

## API Reference

### Microsoft VAAC API

- **Endpoint**: `https://api.interfaces.records.teams.microsoft.com/Teams.VoiceAnalytics/getanalytics`
- **Authentication**: OAuth 2.0 (password grant flow)
- **Query Format**: GZIP-compressed, Base64-encoded, URL-encoded JSON
- **Response Format**: Ordered arrays in `dataResult` field

### Documentation

- [Microsoft Teams AA/CQ Historical Reports](https://learn.microsoft.com/en-us/microsoftteams/aa-cq-cqd-historical-reports)
- [Splunk UCC Framework](https://splunk.github.io/addonfactory-ucc-generator/)
- [Splunk Modular Inputs](https://dev.splunk.com/enterprise/docs/devtools/python/sdk-python/howtousesplunkpython/howtocreatemodinput/)


