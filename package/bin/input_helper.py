import json
import logging
import gzip
import base64
import urllib.parse

import import_declare_test
import requests
from solnlib import conf_manager, log
from splunklib import modularinput as smi


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


def get_oauth_token(logger: logging.Logger, email: str, password: str, tenant_id: str):
    """
    Authenticate using OAuth password grant flow and return access token.
    """
    logger.info(f"Authenticating with OAuth for tenant: {tenant_id}")

    oauth_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    client_id = "a672d62c-fc7b-4e81-a576-e60dc46e951d"

    payload = {
        "client_id": client_id,
        "scope": "user_impersonation",
        "userName": email,
        "password": password,
        "grant_type": "password",
        "resource": "https://api.interfaces.records.teams.microsoft.com"
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


def get_vaac_analytics(logger: logging.Logger, credentials: dict, json_query: str):
    """
    Call VAAC API with OAuth authentication and return analytics data.
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
            logger.info(f"Successfully retrieved {len(result_data) if isinstance(result_data, list) else 1} records from VAAC API")
            return result_data if isinstance(result_data, list) else [result_data]
        else:
            logger.info("Successfully retrieved data from VAAC API")
            return [data]
    except requests.exceptions.RequestException as e:
        logger.error(f"VAAC API call failed: {str(e)}")
        raise


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

            # Get account credentials
            credentials = get_account_credentials(session_key, input_item.get("account"))

            # Get JSON query and validate
            json_query = input_item.get("json_query")
            if not json_query:
                raise ValueError("JSON query is required for VAAC Analytics input")

            # Fetch VAAC Analytics data
            logger.info("Processing VAAC Analytics input")
            data = get_vaac_analytics(logger, credentials, json_query)
            sourcetype = "msteams:vaac:analytics"

            # Write events to Splunk
            for line in data:
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
                len(data),
                input_item.get("index"),
                account=input_item.get("account"),
            )
            log.modular_input_end(logger, normalized_input_name)
        except Exception as e:
            log.log_exception(logger, e, "vaac_analytics_error", msg_before=f"Exception raised while ingesting VAAC analytics data for {normalized_input_name}: ")
