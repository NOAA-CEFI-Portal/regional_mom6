import sys
import json
import subprocess
from prefect import flow, task
from prefect.logging import get_run_logger

@task
def run_script(script_name, config_file):
    """Run a Python script with a given configuration file.

    Parameters
    ----------
    script_name : str
        script name to run end with .py
    config_file : str
        configuration file to pass to the script end with .json

    Returns
    -------
    stdout or stderr
        The output of the script execution.

    Raises
    ------
    Exception
        script execution failed with error message.
    """
    result = subprocess.run(
        ["python", script_name, config_file],
        capture_output=True,
        text=True,
        check=True
    )
    if result.returncode != 0:
        raise Exception(
            f"Script {script_name} failed with error: {result.stderr}"
        )
    return result.stdout

@flow
def prefect_flow_forecast_workflow(json_config_file):
    """the main workflow to run the scripts in the configuration file.
    """
    
    logger = get_run_logger()
    logger.info("================ Start Prefect Flow Forecast ================")
    # Load configuration
    with open(json_config_file, "r", encoding='utf-8') as f:
        logger.info("flow configuration file %s loaded",json_config_file)
        config = json.load(f)

    # Execute each task in the configuration
    for task_config in config["task1"]:
        script_name = task_config["script"]
        config_file = task_config["config"]
        logger.info("--- running script %s with config %s ---", script_name, config_file)
        output = run_script(script_name, config_file)
        logger.info("Output from %s with config %s: %s",script_name,config_file, output)

if __name__ == "__main__":

    # Ensure a JSON file is provided as an argument
    if len(sys.argv) < 2:
        print("Usage: python prefect_flow_forecast.py xxxx.json")
        sys.exit(1)

    # Get the JSON file path from command-line arguments
    json_setting = sys.argv[1]

    prefect_flow_forecast_workflow(json_setting)
