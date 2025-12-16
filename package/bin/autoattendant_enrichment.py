"""
Microsoft Teams Auto Attendant Data Enrichment

This module enriches raw VAAC API Auto Attendant data with calculated fields and
localized strings.

AUTO ATTENDANT ENRICHMENT OUTPUT COLUMNS
=========================================

RAW FIELDS (preserved from VAAC API):
- rawAutoAttendantIdentity: AA resource account URI
- rawAutoAttendantCallFlow: Call flow states
- rawAutoAttendantCallResult: Final call outcomes
- rawAutoAttendantCallerActionCounts: Number of caller actions
- rawAutoAttendantChainDurationInSecs: Call flow duration
- rawAutoAttendantChainIndex: Call chain segment index
- rawAutoAttendantChainStartTime: Call flow start time
- rawAutoAttendantCount: Number of AAs traversed
- rawAutoAttendantDirectorySearchMethod: Search method used
- rawAutoAttendantId: AA GUID
- rawAutoAttendantTransferAction: Transfer target type
- rawHasAA: Is AA involved in call
- rawTotalCallCount: Call count
- rawPSTNTotalMinutes: PSTN minutes

ENRICHED FIELDS (calculated by this module):
- AARAName: Auto Attendant resource account name (before @)
- AASlicer: Display name for filtering
- AAName: Auto Attendant display name
- AAGUID: Auto Attendant ID
- AACallCount: Call count
- AAChainDurationSeconds: Chain duration
- LanguageCode: Language code (e.g., "en-AU")
- (Additional enrichments can be added as needed)

Note: This is a basic implementation. Additional enrichment logic from PowerQuery
can be added here as requirements evolve.

Source: Based on callqueue_enrichment.py structure
"""

from datetime import datetime
import pytz
import logging


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def extract_aa_ra_name(aa_identity):
    """
    Extract resource account name from Auto Attendant identity.

    Args:
        aa_identity (str): AutoAttendantIdentity (e.g., "AAMainReception@hcf.com.au")

    Returns:
        str: Resource account name (e.g., "AAMainReception")
    """
    if not aa_identity:
        return ""

    if '@' in aa_identity:
        return aa_identity.split('@')[0]
    return aa_identity


def parse_timestamp_to_utc(timestamp_str):
    """
    Parse timestamp string to UTC datetime.

    Args:
        timestamp_str (str): Timestamp string from VAAC API

    Returns:
        datetime: Parsed datetime in UTC timezone
    """
    if not timestamp_str:
        return None

    try:
        # Try parsing with or without 'Z' suffix
        if timestamp_str.endswith('Z'):
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        elif '+' in timestamp_str or timestamp_str.count('-') > 2:
            dt = datetime.fromisoformat(timestamp_str)
        else:
            # Assume UTC if no timezone info
            dt = datetime.fromisoformat(timestamp_str).replace(tzinfo=pytz.UTC)

        # Ensure UTC
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        else:
            dt = dt.astimezone(pytz.UTC)

        return dt
    except Exception:
        return None


# ============================================================================
# MAIN ENRICHMENT ORCHESTRATOR
# ============================================================================

def enrich_autoattendant_data(raw_data_list, config=None, logger=None):
    """
    Enrich raw VAAC API Auto Attendant data with calculated fields.

    This is a basic implementation that preserves raw fields and adds
    essential derived fields. Additional enrichment logic can be added
    as requirements evolve.

    Args:
        raw_data_list (list): List of raw data dictionaries from VAAC API
        config (dict): Configuration dictionary with keys:
            - timezone_offset: str (default "UTC")
            - language_code: str (default "en-AU")
        logger (logging.Logger, optional): Logger instance

    Returns:
        list: List of enriched data dictionaries with AutoAttendant[field] structure
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if config is None:
        config = {}

    # Default configuration
    language_code = config.get('language_code', 'en-AU')

    logger.info(f"Starting Auto Attendant enrichment for {len(raw_data_list)} records")
    logger.debug(f"Enrichment config: language={language_code}")

    enriched_data = []
    failed_count = 0

    for idx, raw_record in enumerate(raw_data_list):
        try:
            enriched = {}

            if idx == 0:
                logger.debug(f"Sample raw AA record (first): {raw_record}")

            # ====================================================================
            # STEP 1: Preserve raw fields with "raw" prefix
            # ====================================================================
            logger.debug(f"Step 1/4: Preserving raw AA fields for record {idx + 1}/{len(raw_data_list)}")
            enriched['AutoAttendant[rawAutoAttendantIdentity]'] = raw_record.get('AutoAttendantIdentity', '')
            enriched['AutoAttendant[rawAutoAttendantCallFlow]'] = raw_record.get('AutoAttendantCallFlow', '')
            enriched['AutoAttendant[rawAutoAttendantCallResult]'] = raw_record.get('AutoAttendantCallResult', '')
            enriched['AutoAttendant[rawAutoAttendantCallerActionCounts]'] = raw_record.get('AutoAttendantCallerActionCounts', 0)
            enriched['AutoAttendant[rawAutoAttendantChainDurationInSecs]'] = raw_record.get('AutoAttendantChainDurationInSecs', 0)
            enriched['AutoAttendant[rawAutoAttendantChainIndex]'] = raw_record.get('AutoAttendantChainIndex', 0)
            enriched['AutoAttendant[rawAutoAttendantChainStartTime]'] = raw_record.get('AutoAttendantChainStartTime', '')
            enriched['AutoAttendant[rawAutoAttendantCount]'] = raw_record.get('AutoAttendantCount', 0)
            enriched['AutoAttendant[rawAutoAttendantDirectorySearchMethod]'] = raw_record.get('AutoAttendantDirectorySearchMethod', '')
            enriched['AutoAttendant[rawAutoAttendantId]'] = raw_record.get('AutoAttendantId', '')
            enriched['AutoAttendant[rawAutoAttendantTransferAction]'] = raw_record.get('AutoAttendantTransferAction', '')
            enriched['AutoAttendant[rawHasAA]'] = raw_record.get('HasAA', '')
            enriched['AutoAttendant[rawTotalCallCount]'] = raw_record.get('TotalCallCount', 1)
            enriched['AutoAttendant[rawPSTNTotalMinutes]'] = raw_record.get('PSTNTotalMinutes', 0)

            # Pass-through identifiers if they exist
            enriched['AutoAttendant[DocumentID]'] = raw_record.get('DocumentID', '')
            enriched['AutoAttendant[ConferenceID]'] = raw_record.get('ConferenceID', '')
            enriched['AutoAttendant[DialogID]'] = raw_record.get('DialogID', '')

            # ====================================================================
            # STEP 2: Extract AA names
            # ====================================================================
            logger.debug(f"Step 2/4: Extracting AA names for record {idx + 1}")
            aa_identity = raw_record.get('AutoAttendantIdentity', '')
            ra_name = extract_aa_ra_name(aa_identity)
            enriched['AutoAttendant[AARAName]'] = ra_name
            enriched['AutoAttendant[AASlicer]'] = ra_name  # Simple mode: use RA name
            enriched['AutoAttendant[AAName]'] = ''  # No lookup in simple mode
            logger.debug(f"AA names: Identity={aa_identity}, RAName={ra_name}")

            # ====================================================================
            # STEP 3: Copy/rename other fields
            # ====================================================================
            logger.debug(f"Step 3/4: Copying/renaming AA fields for record {idx + 1}")
            enriched['AutoAttendant[AAGUID]'] = raw_record.get('AutoAttendantId', '')
            enriched['AutoAttendant[AACallCount]'] = raw_record.get('TotalCallCount', 1)
            enriched['AutoAttendant[AAChainDurationSeconds]'] = raw_record.get('AutoAttendantChainDurationInSecs', 0)
            enriched['AutoAttendant[AACallFlow]'] = raw_record.get('AutoAttendantCallFlow', '')
            enriched['AutoAttendant[AACallResult]'] = raw_record.get('AutoAttendantCallResult', '')
            enriched['AutoAttendant[AATransferAction]'] = raw_record.get('AutoAttendantTransferAction', '')
            enriched['AutoAttendant[PSTNTotalMinutes]'] = raw_record.get('PSTNTotalMinutes', 0)
            enriched['AutoAttendant[LanguageCode]'] = language_code

            # ====================================================================
            # STEP 4: Parse chain start time if needed
            # ====================================================================
            logger.debug(f"Step 4/4: Parsing AA chain start time for record {idx + 1}")
            chain_start_time = raw_record.get('AutoAttendantChainStartTime', '')
            if chain_start_time:
                chain_start_utc = parse_timestamp_to_utc(chain_start_time)
                enriched['AutoAttendant[AAChainStartTimeUTC]'] = chain_start_utc.isoformat() if chain_start_utc else chain_start_time
                logger.debug(f"Chain start time: {chain_start_time} → {chain_start_utc}")
            else:
                enriched['AutoAttendant[AAChainStartTimeUTC]'] = ''
                logger.debug("No chain start time in record")

            enriched_data.append(enriched)

            if (idx + 1) % 100 == 0:
                logger.info(f"Progress: Enriched {idx + 1}/{len(raw_data_list)} AA records")

        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to enrich AA record {idx + 1}/{len(raw_data_list)}: {str(e)}", exc_info=True)
            logger.debug(f"Failed AA record data: {raw_record}")
            # Continue processing remaining records

    # Final summary
    logger.info(f"AA enrichment complete: {len(enriched_data)} successful, {failed_count} failed")
    if enriched_data and logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Sample enriched AA record (first): {enriched_data[0]}")

    return enriched_data
