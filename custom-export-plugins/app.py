import json
import glob
import os
import importlib.util
import types
import zipfile
from io import BytesIO
import warnings
import sys
import shutil
from imerit_ango.sdk import SDK
from imerit_ango.plugins import ExportPlugin, ExportResponse, run


HOST = os.environ["HOST"]
PLUGIN_ID = os.environ["PLUGIN_ID"]
PLUGIN_SECRET = os.environ["PLUGIN_SECRET"]


def plugin_caller(**data):
    project_id = data.get("projectId")
    json_export = data.get("jsonExport")
    logger = data.get("logger")
    config_str = data.get("configJSON")
    config = json.loads(config_str)
    if data.get("stage") is None:
        logger.error("You must choose a Stage Filter to run this plugin.")
        restart()

    # Get the script name from the configuration
    script_name = config["script_name"]

    # Get scripts directory path from environment variables
    scripts_dir = os.getcwd() + "/scripts/"

    # Show all scripts listed in the scripts directory
    scripts_list = glob.glob(scripts_dir + "*.py")
    num_scripts = len(scripts_list)
    logger.info(f"Nums of Scripts: {num_scripts}")

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
                    logger.info(f"Error in script: {e}")
                    logger.error(f"Error in script: {e}")
                    restart()
        # return zip_file_name, zip_data
        return ExportResponse(
            file=zip_data,
            file_name=zip_file_name,
        )


# restart the plugin
def restart():
    python = sys.executable
    os.execl(python, python, *sys.argv)


if __name__ == "__main__":
    plugin = ExportPlugin(
        id=PLUGIN_ID,
        secret=PLUGIN_SECRET,
        callback=plugin_caller,
        host=HOST,
        version="v3",
    )
    run(plugin, host=HOST)
