"""
Microsoft Teams Call Queue Data Enrichment

This module enriches raw VAAC API Call Queue data with calculated fields, legend codes,
and localized strings to match PowerBI PowerQuery transformations.

ENRICHMENT OUTPUT COLUMNS (Per Call Mode)
==========================================

RAW FIELDS (preserved from VAAC API):
- rawUserStartTimeUTC: Original call start timestamp (UTC)
- rawEndTime: Original call end timestamp (UTC)
- rawCallQueueId: Call Queue GUID
- rawCallQueueIdentity: Call Queue resource account URI
- rawCallQueueCallResult: Raw call result code
- rawCallQueueTargetType: Raw target type
- rawCallQueueDurationSeconds: Time spent in queue
- rawCallQueueAgentCount: Total agents in queue
- rawCallQueueAgentOptInCount: Agents opted in
- rawPSTNConnectivityType: Connectivity type (CallingPlan/DirectRouting/etc.)
- rawPSTNTotalMinutes: PSTN call duration
- rawTotalCallCount: Call count (usually 1 for per-call)

ENRICHED FIELDS (calculated by this module):
- CQTargetType: Corrected target type (handles callback/timeout special cases)
- CallStartTimeUTC: Parsed ISO timestamp from rawUserStartTimeUTC
- CallEndTimeUTC: Parsed ISO timestamp from rawEndTime
- CallStartTimeLocal: UTC converted to configured timezone
- CallEndTimeLocal: UTC converted to configured timezone
- CallStartDateLocal: Date only from CallStartTimeLocal
- Date: Hourly timestamp (YYYY-MM-DDTHH:00:00)
- DateTimeCQName: Composite key (formatted date + queue name)
- CQConnectivityTypeCode: Numeric code (8600-8620)
- CQConnectivityTypeString: Human-readable connectivity type
- CQCallResultLegendCode: High-level result category (4001-4005, 4999)
- CQCallResultLegendString: Human-readable result category
- CQTargetTypeLegendCode: Detailed disposition code (4010-4034)
- CQTargetTypeLegendString: Human-readable detailed disposition
- CQCallCountAbandoned: 1 if abandoned, 0 otherwise
- CQHour: Hour of day (0-23)
- CQRAName: Queue resource account name (before @)
- CQSlicer: Display name for filtering
- CQGUID: Call Queue ID
- CQName: Call Queue display name (empty in simple mode)
- CQAgentCount: Agent count
- CQAgentOptInCount: Opted-in agent count
- CQCallDurationSeconds: Queue duration
- CQCallCount: Call count
- CQCallResultRaw: Raw result
- CQConnectivityTypeRaw: Raw connectivity type
- PSTNTotalMinutes: PSTN minutes
- ConferenceID, DialogID, DocumentID: Pass-through identifiers
- LanguageCode: Language code (e.g., "en-AU")

Source: PowerQuery M code from powerquery.txt
"""

from datetime import datetime, timedelta
import pytz
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import os


# ============================================================================
# LOOKUP TABLES - HARDCODED (English only)
# ============================================================================

# Connectivity Type Codes
# Source: PowerQuery lines 436-441
CONNECTIVITY_TYPE_CODES = {
    "CallingPlan": 8600,
    "DirectRouting": 8601,
    "OperatorConnect": 8602,
    "ACS Call": 8610,
    "": 8620,  # Blank/Unknown
    None: 8620  # None/Unknown
}

# Connectivity Type Strings (English)
CONNECTIVITY_TYPE_STRINGS = {
    8600: "Calling Plan",
    8601: "Direct Routing",
    8602: "Operator Connect",
    8610: "ACS Call",
    8620: "Unknown"
}

# Call Result Legend Codes (High-level categories)
# Source: PowerQuery lines 476-497
# Maps (CallResult, TargetType) -> LegendCode
CALL_RESULT_LEGEND_STRINGS = {
    4001: "Agent Answered",
    4002: "Overflowed",
    4003: "Timed Out",
    4004: "No Agents",
    4005: "Other",
    4999: "Not Authorized"
}

# Target Type Legend Codes (Detailed disposition)
# Source: PowerQuery lines 499-550
TARGET_TYPE_LEGEND_STRINGS = {
    0: "Not Authorized",
    4005: "Other",
    4010: "Agent Answered (Call)",
    4011: "Agent Answered (Callback)",
    4012: "Abandoned",
    4013: "Overflowed (Application)",
    4014: "Overflowed (Voicemail)",
    4015: "Overflowed (Disconnect)",
    4016: "Overflowed (External)",
    4017: "Overflowed (User)",
    4020: "Timed Out (Application)",
    4021: "Timed Out (Voicemail)",
    4022: "Timed Out (Disconnect)",
    4023: "Timed Out (External)",
    4024: "Timed Out (User)",
    4025: "Timed Out (Callback)",
    4030: "No Agents (Application)",
    4031: "No Agents (Voicemail)",
    4032: "No Agents (Disconnect)",
    4033: "No Agents (External)",
    4034: "No Agents (User)"
}

# Timezone offset mapping
# Supports UTC-12:00 through UTC+14:00 including half-hour and 45-minute zones
TIMEZONE_OFFSETS = {
    "UTC": "+00:00",
    "UTC-12:00": "-12:00",
    "UTC-11:00": "-11:00",
    "UTC-10:00": "-10:00",
    "UTC-09:00": "-09:00",
    "UTC-08:00": "-08:00",
    "UTC-07:00": "-07:00",
    "UTC-06:00": "-06:00",
    "UTC-05:00": "-05:00",
    "UTC-04:00": "-04:00",
    "UTC-03:00": "-03:00",
    "UTC-02:00": "-02:00",
    "UTC-01:00": "-01:00",
    "UTC+01:00": "+01:00",
    "UTC+02:00": "+02:00",
    "UTC+03:00": "+03:00",
    "UTC+03:30": "+03:30",
    "UTC+04:00": "+04:00",
    "UTC+04:30": "+04:30",
    "UTC+05:00": "+05:00",
    "UTC+05:30": "+05:30",
    "UTC+05:45": "+05:45",
    "UTC+06:00": "+06:00",
    "UTC+06:30": "+06:30",
    "UTC+07:00": "+07:00",
    "UTC+08:00": "+08:00",
    "UTC+08:45": "+08:45",
    "UTC+09:00": "+09:00",
    "UTC+09:30": "+09:30",
    "UTC+10:00": "+10:00",
    "UTC+10:30": "+10:30",
    "UTC+11:00": "+11:00",
    "UTC+12:00": "+12:00",
    "UTC+12:45": "+12:45",
    "UTC+13:00": "+13:00",
    "UTC+14:00": "+14:00"
}


# ============================================================================
# ENRICHMENT FUNCTIONS
# ============================================================================

def get_corrected_target_type(call_result, target_type):
    """
    Correct the target type for callback and timeout scenarios.

    Source: PowerQuery lines 310-316

    Logic:
    - callback_call_timed_out → "Disconnect"
    - transferred_to_callback_caller → "User"
    - Otherwise → use original target_type

    Args:
        call_result (str): Raw CallQueueCallResult value
        target_type (str): Raw CallQueueTargetType value

    Returns:
        str: Corrected target type
    """
    if call_result == "callback_call_timed_out":
        return "Disconnect"
    elif call_result == "transferred_to_callback_caller":
        return "User"
    else:
        return target_type if target_type else ""


def parse_timestamp_to_utc(timestamp_str, logger=None):
    """
    Parse timestamp string to UTC datetime.

    Source: PowerQuery lines 319-334

    Args:
        timestamp_str (str): Timestamp string from VAAC API
        logger (logging.Logger, optional): Logger instance

    Returns:
        datetime: Parsed datetime in UTC timezone
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if not timestamp_str:
        logger.debug("Empty timestamp string provided, returning None")
        return None

    try:
        # Try parsing with or without 'Z' suffix
        if timestamp_str.endswith('Z'):
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            logger.debug(f"Parsed timestamp with 'Z' suffix: {timestamp_str} → {dt}")
        elif '+' in timestamp_str or timestamp_str.count('-') > 2:
            dt = datetime.fromisoformat(timestamp_str)
            logger.debug(f"Parsed timestamp with timezone: {timestamp_str} → {dt}")
        else:
            # Assume UTC if no timezone info
            dt = datetime.fromisoformat(timestamp_str).replace(tzinfo=pytz.UTC)
            logger.debug(f"Parsed timestamp assuming UTC: {timestamp_str} → {dt}")

        # Ensure UTC
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        else:
            dt = dt.astimezone(pytz.UTC)

        return dt
    except Exception as e:
        logger.warning(f"Failed to parse timestamp '{timestamp_str}': {str(e)}")
        return None


def convert_to_local_timezone(utc_dt, timezone_offset, logger=None):
    """
    Convert UTC datetime to local timezone.

    Supports both:
    1. Timezone names (e.g., "Australia/Sydney") - automatically handles DST
    2. Legacy fixed offsets (e.g., "UTC+10:00") - for backward compatibility

    Source: PowerQuery lines 337-429 (enhanced with auto DST support)

    Args:
        utc_dt (datetime): UTC datetime
        timezone_offset (str): Timezone name or offset string
        logger (logging.Logger, optional): Logger instance

    Returns:
        datetime: Datetime in local timezone
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if not utc_dt:
        logger.debug("No UTC datetime provided, returning None")
        return None

    try:
        # Detect format: timezone name contains "/", fixed offset does not
        if "/" in timezone_offset:
            # TIMEZONE NAME FORMAT (e.g., "Australia/Sydney")
            # Automatically handles DST transitions
            logger.debug(f"Using timezone name: {timezone_offset} (auto DST)")
            tz = pytz.timezone(timezone_offset)
            local_dt = utc_dt.astimezone(tz)
            logger.debug(f"Converted {utc_dt} to {timezone_offset}: {local_dt} (offset: {local_dt.strftime('%z')})")
            return local_dt
        else:
            # LEGACY FIXED OFFSET FORMAT (e.g., "UTC+10:00")
            # For backward compatibility with existing configurations
            logger.debug(f"Using legacy fixed offset: {timezone_offset}")

            # Get the offset string
            offset_str = TIMEZONE_OFFSETS.get(timezone_offset, "+00:00")
            if timezone_offset not in TIMEZONE_OFFSETS:
                logger.warning(f"Unknown timezone offset '{timezone_offset}', using UTC (+00:00)")

            # Parse offset
            if offset_str.startswith('+'):
                sign = 1
                offset_str = offset_str[1:]
            else:
                sign = -1
                offset_str = offset_str[1:]

            # Split hours and minutes
            if ':' in offset_str:
                hours, minutes = offset_str.split(':')
                hours = int(hours)
                minutes = int(minutes)
            else:
                hours = int(offset_str)
                minutes = 0

            # Create timezone
            total_minutes = sign * (hours * 60 + minutes)
            tz = pytz.FixedOffset(total_minutes)

            # Convert
            local_dt = utc_dt.astimezone(tz)
            logger.debug(f"Converted to local time: {local_dt}")
            return local_dt
    except Exception as e:
        logger.error(f"Failed to convert timezone for {utc_dt} with offset {timezone_offset}: {str(e)}")
        logger.warning("Returning UTC datetime as fallback")
        return utc_dt


def get_call_result_legend_code(call_result, target_type):
    """
    Map call result to high-level legend code.

    Source: PowerQuery lines 476-497

    Args:
        call_result (str): CallQueueCallResult value
        target_type (str): CQTargetType value (corrected)

    Returns:
        int: Legend code (4001-4005, 4999)
    """
    if call_result == "NOTAUTHCQ":
        return 4999  # Not Authorized
    elif call_result == "disconnected" and target_type == "Disconnect":
        return 4012  # Actually returns abandoned, but for high-level it's in the 4001-4005 range
    elif call_result in ["agent_joined_conference", "transferred_to_agent", "transferred_to_callback_caller"]:
        return 4001  # Agent Answered
    elif call_result == "overflown":
        return 4002  # Overflowed
    elif call_result in ["timed_out", "callback_call_timed_out"]:
        return 4003  # Timed Out
    elif call_result == "no_agent":
        return 4004  # No Agents
    else:
        return 4005  # Other


def get_target_type_legend_code(call_result, target_type):
    """
    Map call result and target type to detailed legend code.

    Source: PowerQuery lines 499-550

    Args:
        call_result (str): CallQueueCallResult value
        target_type (str): CQTargetType value (corrected)

    Returns:
        int: Detailed legend code (4010-4034)
    """
    # Not Authorized
    if call_result == "NOTAUTHCQ":
        return 0

    # Agent Answered (Call)
    if call_result in ["agent_joined_conference", "transferred_to_agent"] and target_type == "User":
        return 4010

    # Agent Answered (Callback)
    if call_result == "transferred_to_callback_caller" and target_type == "User":
        return 4011

    # Abandoned
    if call_result == "disconnected" and target_type == "Disconnect":
        return 4012

    # Overflowed variants
    if call_result == "overflown":
        if target_type in ["ApplicationEndpoint", "ConfigurationEndpoint"]:
            return 4013  # Application
        elif target_type == "MailBox":
            return 4014  # Voicemail
        elif target_type == "Disconnect":
            return 4015  # Disconnect
        elif target_type == "Phone":
            return 4016  # External
        elif target_type == "User":
            return 4017  # User

    # Timed Out variants
    if call_result == "timed_out":
        if target_type in ["ApplicationEndpoint", "ConfigurationEndpoint"]:
            return 4020  # Application
        elif target_type == "MailBox":
            return 4021  # Voicemail
        elif target_type == "Disconnect":
            return 4022  # Disconnect
        elif target_type == "Phone":
            return 4023  # External
        elif target_type == "User":
            return 4024  # User

    # Timed Out (Callback)
    if call_result == "callback_call_timed_out" and target_type == "Disconnect":
        return 4025

    # No Agents variants
    if call_result == "no_agent":
        if target_type in ["ApplicationEndpoint", "ConfigurationEndpoint"]:
            return 4030  # Application
        elif target_type == "MailBox":
            return 4031  # Voicemail
        elif target_type == "Disconnect":
            return 4032  # Disconnect
        elif target_type == "Phone":
            return 4033  # External
        elif target_type == "User":
            return 4034  # User

    # Default to Other
    return 4005


def calculate_abandoned_count(call_result, target_type):
    """
    Calculate if call was abandoned.

    Source: PowerQuery lines 446-450

    Args:
        call_result (str): CallQueueCallResult value
        target_type (str): CQTargetType value (corrected)

    Returns:
        int: 1 if abandoned, 0 otherwise
    """
    if call_result == "disconnected" and target_type == "Disconnect":
        return 1
    return 0


def extract_queue_ra_name(queue_identity):
    """
    Extract resource account name from queue identity.

    Source: PowerQuery line 455

    Args:
        queue_identity (str): CallQueueIdentity (e.g., "CQBrookvaleEyecare@hcf.com.au")

    Returns:
        str: Resource account name (e.g., "CQBrookvaleEyecare")
    """
    if not queue_identity:
        return ""

    if '@' in queue_identity:
        return queue_identity.split('@')[0]
    return queue_identity


def format_datetime_cqname(local_dt, ra_name):
    """
    Create composite key from datetime and queue name.

    Source: PowerQuery line 473 + Date format from output_expected.json

    Args:
        local_dt (datetime): Local datetime
        ra_name (str): Resource account name

    Returns:
        str: Formatted string like "28/11/2025 8:00:00 AMCQBrookvaleEyecare"
    """
    if not local_dt:
        return ra_name

    # Format: "DD/MM/YYYY H:MM:SS AM/PM" + CQRAName
    formatted_date = local_dt.strftime('%-d/%-m/%Y %-I:%M:%S %p')
    return f"{formatted_date}{ra_name}"


# ============================================================================
# SINGLE RECORD ENRICHMENT (for parallel processing)
# ============================================================================

def enrich_single_callqueue_record(record_data):
    """
    Enrich a single Call Queue record. Designed for parallel processing.

    Args:
        record_data (tuple): (idx, raw_record, config) where:
            - idx: Record index
            - raw_record: Raw data dictionary
            - config: Enrichment configuration dictionary

    Returns:
        tuple: (idx, enriched_record, success, error_message)
    """
    idx, raw_record, config = record_data

    # Extract config
    timezone_offset = config.get('timezone_offset', 'UTC')
    language_code = config.get('language_code', 'en-AU')
    enable_legend_codes = config.get('enable_legend_codes', True)
    enable_legend_strings = config.get('enable_legend_strings', True)
    enable_timezone_conversion = config.get('enable_timezone_conversion', True)

    try:
        enriched = {}

        # ====================================================================
        # STEP 1: Preserve raw fields with "raw" prefix
        # ====================================================================
        enriched['CallQueue[rawUserStartTimeUTC]'] = raw_record.get('UserStartTimeUTC', '')
        enriched['CallQueue[rawEndTime]'] = raw_record.get('EndTime', '')
        enriched['CallQueue[rawCallQueueId]'] = raw_record.get('CallQueueId', '')
        enriched['CallQueue[rawCallQueueIdentity]'] = raw_record.get('CallQueueIdentity', '')
        enriched['CallQueue[rawCallQueueCallResult]'] = raw_record.get('CallQueueCallResult', '')
        enriched['CallQueue[rawCallQueueTargetType]'] = raw_record.get('CallQueueTargetType', '')
        enriched['CallQueue[rawCallQueueDurationSeconds]'] = raw_record.get('CallQueueDurationSeconds', 0)
        enriched['CallQueue[rawCallQueueAgentCount]'] = raw_record.get('CallQueueAgentCount', 0)
        enriched['CallQueue[rawCallQueueAgentOptInCount]'] = raw_record.get('CallQueueAgentOptInCount', 0)
        enriched['CallQueue[rawPSTNConnectivityType]'] = raw_record.get('PSTNConnectivityType', '')
        enriched['CallQueue[rawPSTNTotalMinutes]'] = raw_record.get('PSTNTotalMinutes', 0)
        enriched['CallQueue[rawTotalCallCount]'] = raw_record.get('TotalCallCount', 1)

        enriched['CallQueue[DocumentID]'] = raw_record.get('DocumentID', '')
        enriched['CallQueue[ConferenceID]'] = raw_record.get('ConferenceID', '')
        enriched['CallQueue[DialogID]'] = raw_record.get('DialogID', '')

        # ====================================================================
        # STEP 2: Calculate CQTargetType (corrected)
        # ====================================================================
        raw_call_result = raw_record.get('CallQueueCallResult', '')
        raw_target_type = raw_record.get('CallQueueTargetType', '')
        cq_target_type = get_corrected_target_type(raw_call_result, raw_target_type)
        enriched['CallQueue[CQTargetType]'] = cq_target_type

        # ====================================================================
        # STEP 3: Parse timestamps to UTC
        # ====================================================================
        call_start_utc = parse_timestamp_to_utc(raw_record.get('UserStartTimeUTC', ''))
        call_end_utc = parse_timestamp_to_utc(raw_record.get('EndTime', ''))

        enriched['CallQueue[CallStartTimeUTC]'] = call_start_utc.isoformat() if call_start_utc else ''
        enriched['CallQueue[CallEndTimeUTC]'] = call_end_utc.isoformat() if call_end_utc else ''

        # ====================================================================
        # STEP 4: Convert to local timezone
        # ====================================================================
        if enable_timezone_conversion and call_start_utc:
            call_start_local = convert_to_local_timezone(call_start_utc, timezone_offset)
            call_end_local = convert_to_local_timezone(call_end_utc, timezone_offset) if call_end_utc else None
        else:
            call_start_local = call_start_utc
            call_end_local = call_end_utc

        enriched['CallQueue[CallStartTimeLocal]'] = call_start_local.isoformat() if call_start_local else ''
        enriched['CallQueue[CallEndTimeLocal]'] = call_end_local.isoformat() if call_end_local else ''

        # ====================================================================
        # STEP 5: Derive date fields
        # ====================================================================
        if call_start_local:
            call_start_date = call_start_local.replace(hour=0, minute=0, second=0, microsecond=0)
            enriched['CallQueue[CallStartDateLocal]'] = call_start_date.isoformat()

            hourly_timestamp = call_start_local.replace(minute=0, second=0, microsecond=0)
            enriched['CallQueue[Date]'] = hourly_timestamp.isoformat()

            enriched['CallQueue[CQHour]'] = call_start_local.hour
        else:
            enriched['CallQueue[CallStartDateLocal]'] = ''
            enriched['CallQueue[Date]'] = ''
            enriched['CallQueue[CQHour]'] = 0

        # ====================================================================
        # STEP 6: Map connectivity type
        # ====================================================================
        raw_connectivity = raw_record.get('PSTNConnectivityType', '')
        connectivity_code = CONNECTIVITY_TYPE_CODES.get(raw_connectivity, 8620)
        enriched['CallQueue[CQConnectivityTypeCode]'] = connectivity_code
        enriched['CallQueue[CQConnectivityTypeString]'] = CONNECTIVITY_TYPE_STRINGS.get(connectivity_code, "Unknown")
        enriched['CallQueue[CQConnectivityTypeRaw]'] = raw_connectivity

        # ====================================================================
        # STEP 7: Calculate legend codes
        # ====================================================================
        if enable_legend_codes:
            call_result_code = get_call_result_legend_code(raw_call_result, cq_target_type)
            target_type_code = get_target_type_legend_code(raw_call_result, cq_target_type)
            enriched['CallQueue[CQCallResultLegendCode]'] = call_result_code
            enriched['CallQueue[CQTargetTypeLegendCode]'] = target_type_code

            if enable_legend_strings:
                enriched['CallQueue[CQCallResultLegendString]'] = CALL_RESULT_LEGEND_STRINGS.get(call_result_code, "Unknown")
                enriched['CallQueue[CQTargetTypeLegendString]'] = TARGET_TYPE_LEGEND_STRINGS.get(target_type_code, "Unknown")
            else:
                enriched['CallQueue[CQCallResultLegendString]'] = ''
                enriched['CallQueue[CQTargetTypeLegendString]'] = ''
        else:
            enriched['CallQueue[CQCallResultLegendCode]'] = 0
            enriched['CallQueue[CQTargetTypeLegendCode]'] = 0
            enriched['CallQueue[CQCallResultLegendString]'] = ''
            enriched['CallQueue[CQTargetTypeLegendString]'] = ''

        # ====================================================================
        # STEP 8: Calculate abandoned count
        # ====================================================================
        enriched['CallQueue[CQCallCountAbandoned]'] = calculate_abandoned_count(raw_call_result, cq_target_type)

        # ====================================================================
        # STEP 9: Extract queue names
        # ====================================================================
        queue_identity = raw_record.get('CallQueueIdentity', '')
        ra_name = extract_queue_ra_name(queue_identity)
        enriched['CallQueue[CQRAName]'] = ra_name
        enriched['CallQueue[CQSlicer]'] = ra_name
        enriched['CallQueue[CQName]'] = ''

        # ====================================================================
        # STEP 10: Create composite key
        # ====================================================================
        enriched['CallQueue[DateTimeCQName]'] = format_datetime_cqname(call_start_local, ra_name)

        # ====================================================================
        # STEP 11: Copy/rename other fields
        # ====================================================================
        enriched['CallQueue[CQGUID]'] = raw_record.get('CallQueueId', '')
        enriched['CallQueue[CQAgentCount]'] = raw_record.get('CallQueueAgentCount', 0)
        enriched['CallQueue[CQAgentOptInCount]'] = raw_record.get('CallQueueAgentOptInCount', 0)
        enriched['CallQueue[CQCallDurationSeconds]'] = raw_record.get('CallQueueDurationSeconds', 0)
        enriched['CallQueue[CQCallCount]'] = raw_record.get('TotalCallCount', 1)
        enriched['CallQueue[CQCallResultRaw]'] = raw_call_result
        enriched['CallQueue[PSTNTotalMinutes]'] = raw_record.get('PSTNTotalMinutes', 0)
        enriched['CallQueue[LanguageCode]'] = language_code

        return (idx, enriched, True, None)

    except Exception as e:
        return (idx, None, False, str(e))


# ============================================================================
# MAIN ENRICHMENT ORCHESTRATOR
# ============================================================================

def enrich_callqueue_data(raw_data_list, config=None, logger=None):
    """
    Enrich raw VAAC API Call Queue data with all calculated fields.

    This function applies all PowerQuery transformations to match the PowerBI
    enrichment logic and output format.

    Args:
        raw_data_list (list): List of raw data dictionaries from VAAC API
        config (dict): Configuration dictionary with keys:
            - timezone_offset: str (default "UTC")
            - language_code: str (default "en-AU")
            - enable_legend_codes: bool (default True)
            - enable_legend_strings: bool (default True)
            - enable_timezone_conversion: bool (default True)
        logger (logging.Logger, optional): Logger instance

    Returns:
        list: List of enriched data dictionaries with CallQueue[field] structure
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if config is None:
        config = {}

    logger.info(f"Starting Call Queue enrichment for {len(raw_data_list)} records")
    logger.debug(f"Enrichment config: {config}")

    # Determine if we should use parallel processing
    parallel_workers = config.get('parallel_workers', 1)
    use_parallel = parallel_workers > 1 and len(raw_data_list) >= 100

    if use_parallel:
        logger.info(f"Using parallel processing with {parallel_workers} workers for {len(raw_data_list)} records")
        return _enrich_callqueue_data_parallel(raw_data_list, config, logger, parallel_workers)
    else:
        logger.info(f"Using sequential processing for {len(raw_data_list)} records")
        return _enrich_callqueue_data_sequential(raw_data_list, config, logger)


def _enrich_callqueue_data_parallel(raw_data_list, config, logger, parallel_workers):
    """
    Enrich Call Queue data using parallel processing.

    Args:
        raw_data_list (list): List of raw data dictionaries
        config (dict): Enrichment configuration
        logger (logging.Logger): Logger instance
        parallel_workers (int): Number of parallel workers

    Returns:
        list: List of enriched data dictionaries
    """
    enriched_data = []
    failed_count = 0

    # Prepare data for parallel processing
    tasks = [(idx, record, config) for idx, record in enumerate(raw_data_list)]

    # Process records in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        # Submit all tasks
        futures = {executor.submit(enrich_single_callqueue_record, task): task[0] for task in tasks}

        # Collect results as they complete
        results = {}
        completed = 0
        for future in as_completed(futures):
            idx, enriched, success, error_msg = future.result()
            results[idx] = (enriched, success, error_msg)

            completed += 1
            if completed % 100 == 0:
                logger.info(f"Progress: Completed {completed}/{len(raw_data_list)} records")

            if not success:
                failed_count += 1
                logger.error(f"Failed to enrich record {idx + 1}: {error_msg}")

    # Sort results by index to maintain order
    for idx in sorted(results.keys()):
        enriched, success, _ = results[idx]
        if success:
            enriched_data.append(enriched)

    # Final summary
    logger.info(f"Parallel enrichment complete: {len(enriched_data)} successful, {failed_count} failed")
    if enriched_data and logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Sample enriched record (first): {enriched_data[0]}")

    return enriched_data


def _enrich_callqueue_data_sequential(raw_data_list, config, logger):
    """
    Enrich Call Queue data using sequential processing (original implementation).

    Args:
        raw_data_list (list): List of raw data dictionaries
        config (dict): Enrichment configuration
        logger (logging.Logger): Logger instance

    Returns:
        list: List of enriched data dictionaries
    """
    # Extract config
    timezone_offset = config.get('timezone_offset', 'UTC')
    language_code = config.get('language_code', 'en-AU')
    enable_legend_codes = config.get('enable_legend_codes', True)
    enable_legend_strings = config.get('enable_legend_strings', True)
    enable_timezone_conversion = config.get('enable_timezone_conversion', True)

    enriched_data = []
    failed_count = 0
    timestamp_parse_success = 0
    timestamp_parse_fail = 0

    for idx, raw_record in enumerate(raw_data_list):
        try:
            enriched = {}

            if idx == 0:
                logger.debug(f"Sample raw record (first): {raw_record}")

            # ====================================================================
            # STEP 1: Preserve raw fields with "raw" prefix
            # ====================================================================
            logger.debug(f"Step 1/11: Preserving raw fields for record {idx + 1}/{len(raw_data_list)}")
            enriched['CallQueue[rawUserStartTimeUTC]'] = raw_record.get('UserStartTimeUTC', '')
            enriched['CallQueue[rawEndTime]'] = raw_record.get('EndTime', '')
            enriched['CallQueue[rawCallQueueId]'] = raw_record.get('CallQueueId', '')
            enriched['CallQueue[rawCallQueueIdentity]'] = raw_record.get('CallQueueIdentity', '')
            enriched['CallQueue[rawCallQueueCallResult]'] = raw_record.get('CallQueueCallResult', '')
            enriched['CallQueue[rawCallQueueTargetType]'] = raw_record.get('CallQueueTargetType', '')
            enriched['CallQueue[rawCallQueueDurationSeconds]'] = raw_record.get('CallQueueDurationSeconds', 0)
            enriched['CallQueue[rawCallQueueAgentCount]'] = raw_record.get('CallQueueAgentCount', 0)
            enriched['CallQueue[rawCallQueueAgentOptInCount]'] = raw_record.get('CallQueueAgentOptInCount', 0)
            enriched['CallQueue[rawPSTNConnectivityType]'] = raw_record.get('PSTNConnectivityType', '')
            enriched['CallQueue[rawPSTNTotalMinutes]'] = raw_record.get('PSTNTotalMinutes', 0)
            enriched['CallQueue[rawTotalCallCount]'] = raw_record.get('TotalCallCount', 1)

            # Pass-through identifiers
            enriched['CallQueue[DocumentID]'] = raw_record.get('DocumentID', '')
            enriched['CallQueue[ConferenceID]'] = raw_record.get('ConferenceID', '')
            enriched['CallQueue[DialogID]'] = raw_record.get('DialogID', '')

            # ====================================================================
            # STEP 2: Calculate CQTargetType (corrected)
            # ====================================================================
            logger.debug(f"Step 2/11: Calculating corrected CQTargetType for record {idx + 1}")
            raw_call_result = raw_record.get('CallQueueCallResult', '')
            raw_target_type = raw_record.get('CallQueueTargetType', '')
            cq_target_type = get_corrected_target_type(raw_call_result, raw_target_type)
            enriched['CallQueue[CQTargetType]'] = cq_target_type
            logger.debug(f"CQTargetType: {raw_target_type} → {cq_target_type}")

            # ====================================================================
            # STEP 3: Parse timestamps to UTC
            # ====================================================================
            logger.debug(f"Step 3/11: Parsing timestamps to UTC for record {idx + 1}")
            call_start_utc = parse_timestamp_to_utc(raw_record.get('UserStartTimeUTC', ''), logger)
            call_end_utc = parse_timestamp_to_utc(raw_record.get('EndTime', ''), logger)

            if call_start_utc:
                timestamp_parse_success += 1
            else:
                timestamp_parse_fail += 1

            enriched['CallQueue[CallStartTimeUTC]'] = call_start_utc.isoformat() if call_start_utc else ''
            enriched['CallQueue[CallEndTimeUTC]'] = call_end_utc.isoformat() if call_end_utc else ''

            # ====================================================================
            # STEP 4: Convert to local timezone
            # ====================================================================
            logger.debug(f"Step 4/11: Converting to local timezone for record {idx + 1}")
            if enable_timezone_conversion and call_start_utc:
                call_start_local = convert_to_local_timezone(call_start_utc, timezone_offset, logger)
                call_end_local = convert_to_local_timezone(call_end_utc, timezone_offset, logger) if call_end_utc else None
            else:
                call_start_local = call_start_utc
                call_end_local = call_end_utc
                if not enable_timezone_conversion:
                    logger.debug("Timezone conversion disabled, using UTC timestamps")

            enriched['CallQueue[CallStartTimeLocal]'] = call_start_local.isoformat() if call_start_local else ''
            enriched['CallQueue[CallEndTimeLocal]'] = call_end_local.isoformat() if call_end_local else ''

            # ====================================================================
            # STEP 5: Derive date fields
            # ====================================================================
            logger.debug(f"Step 5/11: Deriving date fields for record {idx + 1}")
            if call_start_local:
                # CallStartDateLocal: Date only
                call_start_date = call_start_local.replace(hour=0, minute=0, second=0, microsecond=0)
                enriched['CallQueue[CallStartDateLocal]'] = call_start_date.isoformat()

                # Date: Hourly timestamp
                hourly_timestamp = call_start_local.replace(minute=0, second=0, microsecond=0)
                enriched['CallQueue[Date]'] = hourly_timestamp.isoformat()

                # CQHour: Hour of day
                enriched['CallQueue[CQHour]'] = call_start_local.hour
                logger.debug(f"Date fields: Date={hourly_timestamp.isoformat()}, Hour={call_start_local.hour}")
            else:
                enriched['CallQueue[CallStartDateLocal]'] = ''
                enriched['CallQueue[Date]'] = ''
                enriched['CallQueue[CQHour]'] = 0
                logger.debug("No call_start_local available, using empty date fields")

            # ====================================================================
            # STEP 6: Map connectivity type
            # ====================================================================
            logger.debug(f"Step 6/11: Mapping connectivity type for record {idx + 1}")
            raw_connectivity = raw_record.get('PSTNConnectivityType', '')
            connectivity_code = CONNECTIVITY_TYPE_CODES.get(raw_connectivity, 8620)
            enriched['CallQueue[CQConnectivityTypeCode]'] = connectivity_code
            enriched['CallQueue[CQConnectivityTypeString]'] = CONNECTIVITY_TYPE_STRINGS.get(connectivity_code, "Unknown")
            enriched['CallQueue[CQConnectivityTypeRaw]'] = raw_connectivity
            logger.debug(f"Connectivity: {raw_connectivity} → {connectivity_code}")

            # ====================================================================
            # STEP 7: Calculate legend codes
            # ====================================================================
            logger.debug(f"Step 7/11: Calculating legend codes for record {idx + 1}")
            if enable_legend_codes:
                call_result_code = get_call_result_legend_code(raw_call_result, cq_target_type)
                target_type_code = get_target_type_legend_code(raw_call_result, cq_target_type)
                enriched['CallQueue[CQCallResultLegendCode]'] = call_result_code
                enriched['CallQueue[CQTargetTypeLegendCode]'] = target_type_code
                logger.debug(f"Legend codes: CallResult={call_result_code}, TargetType={target_type_code}")

                # Lookup legend strings
                if enable_legend_strings:
                    enriched['CallQueue[CQCallResultLegendString]'] = CALL_RESULT_LEGEND_STRINGS.get(call_result_code, "Unknown")
                    enriched['CallQueue[CQTargetTypeLegendString]'] = TARGET_TYPE_LEGEND_STRINGS.get(target_type_code, "Unknown")
                else:
                    enriched['CallQueue[CQCallResultLegendString]'] = ''
                    enriched['CallQueue[CQTargetTypeLegendString]'] = ''
            else:
                logger.debug("Legend codes disabled")
                enriched['CallQueue[CQCallResultLegendCode]'] = 0
                enriched['CallQueue[CQTargetTypeLegendCode]'] = 0
                enriched['CallQueue[CQCallResultLegendString]'] = ''
                enriched['CallQueue[CQTargetTypeLegendString]'] = ''

            # ====================================================================
            # STEP 8: Calculate abandoned count
            # ====================================================================
            logger.debug(f"Step 8/11: Calculating abandoned count for record {idx + 1}")
            enriched['CallQueue[CQCallCountAbandoned]'] = calculate_abandoned_count(raw_call_result, cq_target_type)

            # ====================================================================
            # STEP 9: Extract queue names
            # ====================================================================
            logger.debug(f"Step 9/11: Extracting queue names for record {idx + 1}")
            queue_identity = raw_record.get('CallQueueIdentity', '')
            ra_name = extract_queue_ra_name(queue_identity)
            enriched['CallQueue[CQRAName]'] = ra_name
            enriched['CallQueue[CQSlicer]'] = ra_name  # Simple mode: use RA name
            enriched['CallQueue[CQName]'] = ''  # No lookup in simple mode
            logger.debug(f"Queue names: Identity={queue_identity}, RAName={ra_name}")

            # ====================================================================
            # STEP 10: Create composite key
            # ====================================================================
            logger.debug(f"Step 10/11: Creating composite key for record {idx + 1}")
            enriched['CallQueue[DateTimeCQName]'] = format_datetime_cqname(call_start_local, ra_name)

            # ====================================================================
            # STEP 11: Copy/rename other fields
            # ====================================================================
            logger.debug(f"Step 11/11: Copying/renaming final fields for record {idx + 1}")
            enriched['CallQueue[CQGUID]'] = raw_record.get('CallQueueId', '')
            enriched['CallQueue[CQAgentCount]'] = raw_record.get('CallQueueAgentCount', 0)
            enriched['CallQueue[CQAgentOptInCount]'] = raw_record.get('CallQueueAgentOptInCount', 0)
            enriched['CallQueue[CQCallDurationSeconds]'] = raw_record.get('CallQueueDurationSeconds', 0)
            enriched['CallQueue[CQCallCount]'] = raw_record.get('TotalCallCount', 1)
            enriched['CallQueue[CQCallResultRaw]'] = raw_call_result
            enriched['CallQueue[PSTNTotalMinutes]'] = raw_record.get('PSTNTotalMinutes', 0)
            enriched['CallQueue[LanguageCode]'] = language_code

            enriched_data.append(enriched)

            if (idx + 1) % 100 == 0:
                logger.info(f"Progress: Enriched {idx + 1}/{len(raw_data_list)} records")

        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to enrich record {idx + 1}/{len(raw_data_list)}: {str(e)}", exc_info=True)
            logger.debug(f"Failed record data: {raw_record}")
            # Continue processing remaining records

    # Final summary
    logger.info(f"Enrichment complete: {len(enriched_data)} successful, {failed_count} failed")
    logger.info(f"Timestamp parsing: {timestamp_parse_success} successful, {timestamp_parse_fail} failed")
    if enriched_data and logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Sample enriched record (first): {enriched_data[0]}")

    return enriched_data
