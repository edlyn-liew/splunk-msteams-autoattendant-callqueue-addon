import json
import logging

import import_declare_test
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
        "password": account.get("password")
    }


def get_data_from_api(logger: logging.Logger, credentials: dict):
    logger.info("Getting data from an external API")
    email = credentials.get("email")
    password = credentials.get("password")

    # TODO: Implement actual API call using email and password for authentication
    logger.info(f"Authenticating with email: {email}")

    dummy_data = [
        {
            "line1": "hello",
        },
        {
            "line2": "world",
        },
    ]
    return dummy_data


def validate_input(definition: smi.ValidationDefinition):
    return


def stream_events(inputs: smi.InputDefinition, event_writer: smi.EventWriter):
    # inputs.inputs is a Python dictionary object like:
    # {
    #   "input://<input_name>": {
    #     "account": "<account_name>",
    #     "disabled": "0",
    #     "host": "$decideOnStartup",
    #     "index": "<index_name>",
    #     "interval": "<interval_value>",
    #     "python.version": "python3",
    #   },
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
            credentials = get_account_credentials(session_key, input_item.get("account"))
            data = get_data_from_api(logger, credentials)
            sourcetype = "dummy-data"
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
            log.log_exception(logger, e, "my custom error type", msg_before="Exception raised while ingesting data for demo_input: ")
