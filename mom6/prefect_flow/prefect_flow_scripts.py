import sys
import json
import subprocess
from prefect import flow, task
from prefect.logging import get_run_logger

@task(retry_delay_seconds=60, retries=100, timeout_seconds=2*60*60)
def run_script(script_name, config_file):
    """Run a Python script with a given configuration file.

    current setting is to run the script for 24 hours and retry 5 times with 1 min delay.
    make sure the script can be forced to stop and restart without rerunning the task
    but continue

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
    logger = get_run_logger()
    result = subprocess.run(
        ["python", script_name, config_file],
        capture_output=True,
        text=True,
        check=True
    )
    if result.returncode != 0:
        logger.error(
            "Script %s with config file %s failed with error: %s",
            script_name,
            config_file,
            result.stderr
        )
        raise Exception(
            f"Script {script_name} with config file {config_file} failed with error: {result.stderr}"
        )
    return result.stdout 

@flow
def prefect_flow_workflow(json_config_file):
    """the main workflow to run the scripts in the configuration file.
    """
    
    logger = get_run_logger()
    logger.info("================ Start Prefect Flow ================")
    # Load configuration
    with open(json_config_file, "r", encoding='utf-8') as f:
        logger.info("flow configuration file %s loaded",json_config_file)
        config = json.load(f)

    # get task names
    task_names = list(config.keys())
    for ntask,task_name in enumerate(task_names):
        logger.info("%s task names %s",ntask,task_name)


    # Execute each task in the configuration
    for task_name in task_names:
        for task_config in config[task_name]:
            script_name = task_config["script"]
            config_file = task_config["config"]
            logger.info("--- running script %s with config %s ---", script_name, config_file)
            output = run_script(script_name, config_file)
            logger.info("Output from %s with config %s: %s",script_name,config_file, output)

if __name__ == "__main__":

    # Ensure a JSON file is provided as an argument
    if len(sys.argv) < 2:
        print("Usage: python prefect_flow_scripts.py xxxx.json")
        sys.exit(1)

    # Get the JSON file path from command-line arguments
    json_setting = sys.argv[1]

    prefect_flow_workflow(json_setting)
