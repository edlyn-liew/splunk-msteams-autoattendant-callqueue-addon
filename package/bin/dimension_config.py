"""
Microsoft Teams VAAC Dimension Configuration

This module contains hardcoded dimension lists for Auto Attendant and Call Queue reporting.
Users select either "Auto Attendant" or "Call Queue" in the Splunk UI, and these
predefined dimension sets are used in the VAAC API query.

Reference: https://learn.microsoft.com/en-us/microsoftteams/aa-cq-cqd-historical-reports
"""

import logging

# Auto Attendant Dimensions
# These dimensions capture data about Auto Attendant call flows and routing
AUTO_ATTENDANT_DIMENSIONS = [
    # Common dimensions (must be first for ordered array mapping)
    "DocumentId",                         # Call identifier
    "ConferenceId",                       # Call identifier
    "DialogId",                           # Call identifier
    "UserStartTimeUTC",                   # Time call started (UTC)
    "EndTime",                            # Time call ended (UTC)
    "Date",                               # Date of call (UTC)
    # Auto Attendant specific dimensions
    "AutoAttendantIdentity",              # Resource account URI
    "AutoAttendantCallFlow",              # Call flow states
    "AutoAttendantCallResult",            # Final call outcomes
    "AutoAttendantCallerActionCounts",    # Number of caller actions
    "AutoAttendantChainDurationInSecs",   # Call flow duration
    "AutoAttendantChainIndex",            # Call chain segment index
    "AutoAttendantChainStartTime",        # Call flow start time
    "AutoAttendantCount",                 # Number of AAs traversed
    "AutoAttendantDirectorySearchMethod", # Search method used
    "AutoAttendantId",                    # AA GUID
    "AutoAttendantTransferAction",        # Transfer target type
    "HasAA"                               # Is AA involved in call
]

# Call Queue Dimensions
# These dimensions capture data about Call Queue operations and outcomes
CALL_QUEUE_DIMENSIONS = [
    # Common dimensions (must be first for ordered array mapping)
    "DocumentId",                         # Call identifier
    "ConferenceId",                       # Call identifier
    "DialogId",                           # Call identifier
    "UserStartTimeUTC",                   # Time call started (UTC)
    "EndTime",                            # Time call ended (UTC)
    "Date",                               # Date of call (UTC)
    # Call Queue specific dimensions
    "CallQueueIdentity",                  # Resource account URI
    "CallQueueAgentCount",                # Number of agents in queue
    "CallQueueAgentOptInCount",           # Number of opted-in agents
    "CallQueueCallResult",                # Final call state
    "CallQueueDurationSeconds",           # Duration in queue
    "CallQueueFinalStateAction",          # Final action taken
    "CallQueueId",                        # Call Queue GUID
    "CallQueueTargetType",                # Call redirection target
    "HasCQ",                              # Is CQ involved in call
    "TransferredFromCQId",                # Source queue GUID (if transferred)
    "TransferredFromCallQueueIdentity"    # Source queue URI (if transferred)
]

# Default Measurements
# These measurements are included for both Auto Attendant and Call Queue reports
DEFAULT_MEASUREMENTS = [
    "PSTNTotalMinutes",                   # Total PSTN call minutes
    "TotalCallCount"                      # Total number of calls
]

# Additional measurements that can be enabled for specific use cases
OPTIONAL_AA_MEASUREMENTS = [
    "AvgAutoAttendantChainDurationSeconds"  # Average AA call flow duration
]

OPTIONAL_CQ_MEASUREMENTS = [
    "AvgCallDuration",                    # Average total call duration
    "AvgCallQueueDurationSeconds"         # Average queue duration
]

# General optional measurements
OPTIONAL_GENERAL_MEASUREMENTS = [
    "TotalAudioStreamDuration"            # Total audio stream duration
]


def get_dimensions_for_report_type(report_type, logger=None):
    """
    Get the appropriate dimension list based on report type.

    Args:
        report_type (str): Either 'auto_attendant' or 'call_queue'
        logger (logging.Logger, optional): Logger instance

    Returns:
        list: List of dimension names for the VAAC API query

    Raises:
        ValueError: If report_type is not recognized
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.debug(f"Getting dimensions for report type: {report_type}")

    if report_type == "auto_attendant":
        dimensions = AUTO_ATTENDANT_DIMENSIONS
        logger.info(f"Retrieved {len(dimensions)} Auto Attendant dimensions")
        logger.debug(f"Auto Attendant dimensions: {', '.join(dimensions)}")
        return dimensions
    elif report_type == "call_queue":
        dimensions = CALL_QUEUE_DIMENSIONS
        logger.info(f"Retrieved {len(dimensions)} Call Queue dimensions")
        logger.debug(f"Call Queue dimensions: {', '.join(dimensions)}")
        return dimensions
    else:
        error_msg = f"Unknown report type: {report_type}. Must be 'auto_attendant' or 'call_queue'"
        logger.error(error_msg)
        raise ValueError(error_msg)


def get_measurements_for_report_type(report_type, include_optional=False, logger=None):
    """
    Get the appropriate measurement list based on report type.

    Args:
        report_type (str): Either 'auto_attendant' or 'call_queue'
        include_optional (bool): Whether to include optional measurements
        logger (logging.Logger, optional): Logger instance

    Returns:
        list: List of measurement names for the VAAC API query
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.debug(f"Getting measurements for report type: {report_type}, include_optional: {include_optional}")

    measurements = DEFAULT_MEASUREMENTS.copy()
    logger.debug(f"Starting with {len(measurements)} default measurements: {', '.join(measurements)}")

    if include_optional:
        if report_type == "auto_attendant":
            measurements.extend(OPTIONAL_AA_MEASUREMENTS)
            logger.debug(f"Added {len(OPTIONAL_AA_MEASUREMENTS)} optional AA measurements")
        elif report_type == "call_queue":
            measurements.extend(OPTIONAL_CQ_MEASUREMENTS)
            logger.debug(f"Added {len(OPTIONAL_CQ_MEASUREMENTS)} optional CQ measurements")
        measurements.extend(OPTIONAL_GENERAL_MEASUREMENTS)
        logger.debug(f"Added {len(OPTIONAL_GENERAL_MEASUREMENTS)} optional general measurements")

    logger.info(f"Retrieved {len(measurements)} total measurements for {report_type}")
    logger.debug(f"Measurements: {', '.join(measurements)}")

    return measurements
