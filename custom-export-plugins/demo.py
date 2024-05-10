import json
import glob
import os
import importlib.util
import types
import zipfile
from io import BytesIO
from ango.sdk import SDK
from ango.plugins import ExportPlugin, run
from dotenv import load_dotenv
import sys
import shutil

load_dotenv()

HOST = "https://api.ango.ai"

# Load env variables
PLUGIN_ID = os.getenv("demo_id")
PLUGIN_SECRET = os.getenv("demo_secret")


def plugin_caller(**data):
    project_id = data.get("projectId")
    json_export = data.get("jsonExport")
    logger = data.get("logger")
    config_str = data.get("configJSON")
    config = json.loads(config_str)

    # Get the script name from the configuration
    script_name = config["script_name"]
    scripts_dir = os.getcwd() + "/scripts/"

    # Use glob to get the script file in the scripts directory
    filename = glob.glob(scripts_dir + f"{script_name}.py")
    logger.info(f"Directory: {scripts_dir}")
    logger.info(f"Looking for script: {script_name}")
    if not filename:
        logger.info(f"No script named {script_name} found. [info]")
        logger.error(f"No script named {script_name} found. [error]")
        logger.info("Stopping plugin execution. [info]")
        logger.error("Stopping plugin execution. [error]")
        restart()
    else:
        filename = filename[0]
        logger.info(f"Found script: {filename}")
        # Create a module spec
        spec = importlib.util.spec_from_file_location(script_name, filename)

        # Initialize the module
        module = importlib.util.module_from_spec(spec)

        # Load the module into memory (this actually executes the module)
        spec.loader.exec_module(module)

        # Find and call the function
        for attr_name in dir(module):
            script_function = getattr(module, attr_name)
            if isinstance(script_function, types.FunctionType):
                try:
                    # call the function and return the results
                    output_folder = script_function(**data)
                    logger.info(f"{script_name} executed successfully.")
                    zip_file_name = project_id + ".zip"
                    zip_data = BytesIO()
                    with zipfile.ZipFile(zip_data, mode="w") as zf:
                        for root, dirs, files in os.walk(output_folder):
                            for file in files:
                                file_path = os.path.join(root, file)
                                relative_path = os.path.relpath(
                                    file_path, output_folder
                                )
                                zf.write(file_path, arcname=relative_path)
                    logger.info(f"Created zip file: {zip_file_name}")
                    shutil.rmtree(output_folder)
                except Exception as e:
                    # handle any exceptions in the invoked script and restart the plugin
                    logger.error(f"Error in script: {e}")
                    restart()
        return zip_file_name, zip_data


# restart the plugin
def restart():
    python = sys.executable
    os.execl(python, python, *sys.argv)


if __name__ == "__main__":
    # plugin_caller()
    plugin = ExportPlugin(
        id=PLUGIN_ID, secret=PLUGIN_SECRET, callback=plugin_caller, host=HOST
    )

    run(plugin, host=HOST)
