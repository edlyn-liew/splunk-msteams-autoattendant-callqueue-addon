import json
import logging
import gzip
import base64
import urllib.parse

import import_declare_test
import requests
from solnlib import conf_manager, log
from solnlib.modular_input import checkpointer
from splunklib import modularinput as smi

# Import dimension configuration
from dimension_config import get_dimensions_for_report_type, get_measurements_for_report_type

# Import enrichment modules
from callqueue_enrichment import enrich_callqueue_data
from autoattendant_enrichment import enrich_autoattendant_data


ADDON_NAME = "splunk_msteams_aa_callqueue_reporting_addon"

def logger_for_input(input_name: str) -> logging.Logger:
    return log.Logs().get_logger(f"{ADDON_NAME.lower()}_{input_name}")


def get_account_credentials(session_key: str, account_name: str):
    cfm = conf_manager.ConfManager(
        session_key,
        ADDON_NAME,
        realm=f"__REST_CREDENTIAL__#{ADDON_NAME}#configs/conf-splunk_msteams_aa_callqueue_reporting_addon_account",
    )
    account_conf_file = cfm.get_conf("splunk_msteams_aa_callqueue_reporting_addon_account")
    account = account_conf_file.get(account_name)
    return {
        "email": account.get("email"),
        "password": account.get("password"),
        "tenant_id": account.get("tenant_id")
    }


def transform_ordered_arrays_to_dicts(array_data, dimensions, measurements):
    """
    Transform VAAC API ordered array responses to dictionary format.

    The VAAC API returns data as ordered arrays where each element's position
    corresponds to a field in the combined dimensions + measurements list.

    Args:
        array_data (list): List of arrays from VAAC dataResult
        dimensions (list): List of dimension names (ordered as in API query)
        measurements (list): List of measurement names (ordered as in API query)

    Returns:
        list: List of dictionaries with field names as keys

    Example:
        dimensions = ["UserStartTimeUTC", "CallQueueIdentity"]
        measurements = ["TotalCallCount"]
        array_data = [["2025-12-15T23:59:41", "CQ@example.com", 1]]

        Returns: [{"UserStartTimeUTC": "2025-12-15T23:59:41",
                   "CallQueueIdentity": "CQ@example.com",
                   "TotalCallCount": 1}]
    """
    field_names = dimensions + measurements
    transformed = []

    for row in array_data:
        record = {}
        for idx, field_name in enumerate(field_names):
            # Use None for missing values if array is shorter than expected
            record[field_name] = row[idx] if idx < len(row) else None
        transformed.append(record)

    return transformed


def get_oauth_token(logger: logging.Logger, email: str, password: str, tenant_id: str):
    """
    Authenticate using OAuth password grant flow and return access token.
    """
    logger.info(f"Authenticating with OAuth for tenant: {tenant_id}")

    oauth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    client_id = "a672d62c-fc7b-4e81-a576-e60dc46e951d"

    payload = {
        "client_id": client_id,
        "scope": "https://api.interfaces.records.teams.microsoft.com/.default",
        "userName": email,
        "password": password,
        "grant_type": "password"
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        response = requests.post(oauth_url, headers=headers, data=payload, timeout=30)
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data.get("access_token")

        if not access_token:
            raise Exception("No access token in OAuth response")

        logger.info("Successfully obtained OAuth access token")
        return access_token
    except requests.exceptions.RequestException as e:
        logger.error(f"OAuth authentication failed: {str(e)}")
        raise


def prepare_vaac_query(logger: logging.Logger, json_query: str):
    """
    Prepare the query for VAAC API:
    1. GZIP compress the JSON query
    2. Base64 encode
    3. URL encode
    Returns: encoded query string
    """
    logger.info("Preparing VAAC query")

    try:
        # GZIP compress
        compressed = gzip.compress(json_query.encode('utf-8'))
        logger.debug(f"Compressed query size: {len(compressed)} bytes")

        # Base64 encode
        b64_encoded = base64.b64encode(compressed)

        # URL encode
        url_encoded = urllib.parse.quote(b64_encoded)

        logger.info("Successfully prepared VAAC query")
        return url_encoded
    except Exception as e:
        logger.error(f"Failed to prepare VAAC query: {str(e)}")
        raise


def get_vaac_analytics(logger: logging.Logger, credentials: dict, json_query: str,
                        dimensions: list, measurements: list):
    """
    Call VAAC API with OAuth authentication and return analytics data.

    Args:
        logger: Logger instance
        credentials: Dictionary with email, password, tenant_id
        json_query: VAAC query JSON string
        dimensions: Ordered list of dimension names for array transformation
        measurements: Ordered list of measurement names for array transformation

    Returns:
        list: List of dictionaries with field names as keys
    """
    logger.info("Fetching VAAC analytics data")

    # Get OAuth token
    access_token = get_oauth_token(
        logger,
        credentials["email"],
        credentials["password"],
        credentials["tenant_id"]
    )

    # Prepare the query
    encoded_query = prepare_vaac_query(logger, json_query)

    # Construct API URL
    api_endpoint = "https://api.interfaces.records.teams.microsoft.com/Teams.VoiceAnalytics/getanalytics"
    api_url = f"{api_endpoint}?query={encoded_query}"

    # Set headers
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    try:
        # Make API request
        logger.info("Calling VAAC API")
        response = requests.get(api_url, headers=headers, timeout=60)
        response.raise_for_status()

        data = response.json()

        # Extract dataResult if it exists
        if "dataResult" in data:
            result_data = data["dataResult"]
            logger.info(f"Successfully retrieved {len(result_data) if isinstance(result_data, list) else 1} array records from VAAC API")

            # Transform ordered arrays to dictionaries
            if isinstance(result_data, list) and len(result_data) > 0:
                logger.info(f"Transforming {len(result_data)} ordered array records to dictionary format")
                transformed_data = transform_ordered_arrays_to_dicts(result_data, dimensions, measurements)
                logger.info(f"Successfully transformed {len(transformed_data)} records")
                return transformed_data
            else:
                return []
        else:
            logger.warning("No dataResult in VAAC API response")
            return []
    except requests.exceptions.RequestException as e:
        logger.error(f"VAAC API call failed: {str(e)}")
        raise


def construct_vaac_query(logger: logging.Logger, input_item: dict, checkpoint_helper=None, input_name=None):
    """
    Construct VAAC JSON query from structured input fields.

    Args:
        logger: Logger instance
        input_item: Dictionary containing input configuration
        checkpoint_helper: Checkpoint helper for tracking last processed datetime
        input_name: Normalized input name for checkpoint key

    Returns:
        tuple: (query_json, end_date_iso, dimensions_list, measurements_list)
            - query_json: JSON string ready for VAAC API
            - end_date_iso: ISO datetime for checkpoint storage
            - dimensions_list: Ordered list of dimension names (for array transformation)
            - measurements_list: Ordered list of measurement names (for array transformation)
    """
    from datetime import datetime, timedelta, timezone

    query = {}

    # Get report type (auto_attendant or call_queue)
    report_type = input_item.get("report_type", "call_queue")
    logger.info(f"Constructing VAAC query for report type: {report_type}")

    # Get hardcoded dimensions for the selected report type
    dimensions_list = get_dimensions_for_report_type(report_type, logger=logger)
    query["Dimensions"] = [{"DataModelName": dim} for dim in dimensions_list]

    # Get hardcoded measurements for the selected report type
    measurements_list = get_measurements_for_report_type(report_type, include_optional=False, logger=logger)
    query["Measurements"] = [{"DataModelName": m} for m in measurements_list]

    # Handle Date and Time Filters with checkpoint support
    # Get current time (UTC, timezone-aware)
    end_date_dt = datetime.now(timezone.utc)
    end_date = end_date_dt.strftime("%Y-%m-%d")
    end_date_iso = end_date_dt.isoformat()  # For checkpoint storage

    # Check for existing checkpoint
    checkpoint_key = f"{input_name}_last_processed" if input_name else None
    last_checkpoint = None

    if checkpoint_helper and checkpoint_key:
        try:
            checkpoint_data = checkpoint_helper.get(checkpoint_key)
            if checkpoint_data and "last_datetime" in checkpoint_data:
                last_checkpoint = checkpoint_data["last_datetime"]
                logger.info(f"Found checkpoint for '{input_name}': {last_checkpoint}")
        except Exception as e:
            logger.warning(f"Failed to retrieve checkpoint for '{input_name}': {str(e)}")

    # Determine start_date and start_datetime
    if last_checkpoint:
        # Use checkpoint as start date (incremental mode)
        start_date_dt = datetime.fromisoformat(last_checkpoint)
        start_date = start_date_dt.strftime("%Y-%m-%d")
        start_datetime_iso = last_checkpoint  # ISO format for UserStartTimeUTC filter
        logger.info(f"Using checkpoint start datetime: {start_datetime_iso} (incremental mode)")
    else:
        # Fallback to interval-based (first run or checkpoint failure)
        interval_seconds = int(input_item.get("interval", 3600))
        start_date_dt = end_date_dt - timedelta(seconds=interval_seconds)
        start_date = start_date_dt.strftime("%Y-%m-%d")
        start_datetime_iso = start_date_dt.isoformat()  # ISO format for UserStartTimeUTC filter
        logger.info(f"No checkpoint found for '{input_name}', using interval-based start datetime: {start_datetime_iso} (lookback: {interval_seconds}s)")

    logger.info(f"Query date range: {start_date} to {end_date}")
    logger.info(f"Query datetime range: {start_datetime_iso} to {end_date_iso}")

    # Build filters with both UserStartTimeUTC (for precise time filtering) and Date (for day boundaries)
    query["Filters"] = [
        {
            "DataModelName": "UserStartTimeUTC",
            "Value": start_datetime_iso,
            "Operand": 4  # Greater than or equal (>=)
        },
        {
            "DataModelName": "Date",
            "Value": start_date,
            "Operand": 4  # Greater than or equal (>=)
        },
        {
            "DataModelName": "Date",
            "Value": end_date,
            "Operand": 6  # Less than or equal (<=)
        }
    ]

    # Handle LimitResultRowsCount
    limit = input_item.get("limit_result_rows", "200000")
    query["LimitResultRowsCount"] = int(limit)

    # Add Parameters
    query["Parameters"] = {
        "UserAgent": "Splunk Add-on for MS Teams AA/CQ Reporting"
    }

    # Return query JSON, checkpoint datetime, and dimension/measurement lists for array transformation
    return json.dumps(query), end_date_iso, dimensions_list, measurements_list


def validate_input(definition: smi.ValidationDefinition):
    return


def stream_events(inputs: smi.InputDefinition, event_writer: smi.EventWriter):
    # inputs.inputs is a Python dictionary object like:
    # {
    #   "vaac_analytics://<input_name>": {
    #     "account": "<account_name>",
    #     "disabled": "0",
    #     "host": "$decideOnStartup",
    #     "index": "<index_name>",
    #     "interval": "<interval_value>",
    #     "json_query": "<json_query>",
    #     "python.version": "python3",
    #   }
    # }
    for input_name, input_item in inputs.inputs.items():
        normalized_input_name = input_name.split("/")[-1]
        logger = logger_for_input(normalized_input_name)
        try:
            session_key = inputs.metadata["session_key"]
            log_level = conf_manager.get_log_level(
                logger=logger,
                session_key=session_key,
                app_name=ADDON_NAME,
                conf_name="splunk_msteams_aa_callqueue_reporting_addon_settings",
            )
            logger.setLevel(log_level)
            log.modular_input_start(logger, normalized_input_name)

            # Initialize checkpoint helper for this input
            checkpoint_helper = checkpointer.KVStoreCheckpointer(
                collection_name="splunk_msteams_checkpoints",
                session_key=session_key,
                app=ADDON_NAME
            )
            logger.debug(f"Initialized checkpoint helper for input: {normalized_input_name}")

            # Get account credentials
            credentials = get_account_credentials(session_key, input_item.get("account"))

            # Construct JSON query from structured fields with checkpoint support
            logger.info("Constructing VAAC query from input fields")
            json_query, end_date_iso, dimensions_list, measurements_list = construct_vaac_query(
                logger, input_item, checkpoint_helper, normalized_input_name
            )
            logger.debug(f"Constructed query: {json_query}")
            logger.debug(f"Query end date (for checkpoint): {end_date_iso}")
            logger.debug(f"Dimensions ({len(dimensions_list)}): {', '.join(dimensions_list[:5])}...")
            logger.debug(f"Measurements ({len(measurements_list)}): {', '.join(measurements_list)}")

            # Fetch VAAC Analytics data
            logger.info("Processing VAAC Analytics input")
            raw_data = get_vaac_analytics(
                logger, credentials, json_query, dimensions_list, measurements_list
            )

            # Get report type for enrichment
            report_type = input_item.get("report_type", "call_queue")

            # Prepare enrichment configuration
            enrichment_config = {
                'timezone_offset': input_item.get('timezone_offset', 'UTC'),
                'language_code': input_item.get('language_code', 'en-AU'),
                'parallel_workers': int(input_item.get('parallel_workers', 4)),
                'enable_legend_codes': True,
                'enable_legend_strings': True,
                'enable_timezone_conversion': True
            }
            logger.debug(f"Enrichment config: parallel_workers={enrichment_config['parallel_workers']}, "
                        f"timezone={enrichment_config['timezone_offset']}")

            # Apply enrichment based on report type
            logger.info(f"Applying {report_type} enrichment to {len(raw_data)} records")
            if report_type == "call_queue":
                enriched_data = enrich_callqueue_data(raw_data, enrichment_config, logger=logger)
                sourcetype = "msteams:vaac:callqueue"
            elif report_type == "auto_attendant":
                enriched_data = enrich_autoattendant_data(raw_data, enrichment_config, logger=logger)
                sourcetype = "msteams:vaac:autoattendant"
            else:
                # Fallback: no enrichment
                logger.warning(f"Unknown report type '{report_type}', skipping enrichment")
                enriched_data = raw_data
                sourcetype = "msteams:vaac:analytics"

            # Write enriched events to Splunk
            logger.info(f"Writing {len(enriched_data)} enriched events to Splunk")
            for line in enriched_data:
                event_writer.write_event(
                    smi.Event(
                        data=json.dumps(line, ensure_ascii=False, default=str),
                        index=input_item.get("index"),
                        sourcetype=sourcetype,
                    )
                )

            log.events_ingested(
                logger,
                input_name,
                sourcetype,
                len(enriched_data),
                input_item.get("index"),
                account=input_item.get("account"),
            )

            # Update checkpoint after successful data ingestion
            if checkpoint_helper and normalized_input_name:
                try:
                    from datetime import datetime, timezone
                    checkpoint_key = f"{normalized_input_name}_last_processed"
                    checkpoint_helper.update(checkpoint_key, {
                        "last_datetime": end_date_iso,
                        "processed_records": len(enriched_data),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                        "report_type": report_type
                    })
                    logger.info(f"Checkpoint updated for '{normalized_input_name}': last_datetime={end_date_iso}, records={len(enriched_data)}")
                except Exception as e:
                    logger.error(f"Failed to update checkpoint for '{normalized_input_name}': {str(e)}")
                    # Don't fail the input - checkpoint update failure is not critical

            log.modular_input_end(logger, normalized_input_name)
        except Exception as e:
            log.log_exception(logger, e, "vaac_analytics_error", msg_before=f"Exception raised while ingesting VAAC analytics data for {normalized_input_name}: ")
