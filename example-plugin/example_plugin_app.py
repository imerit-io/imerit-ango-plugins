import json
import zipfile
from io import BytesIO
from imerit_ango.plugins import ExportPlugin, ExportResponse, run

# HOST = os.environ['HOST']
HOST = "https://imeritapi.ango.ai"
# PLUGIN_ID = os.environ['PLUGIN_ID']
PLUGIN_ID = "6643712666d9fa001508c5e8"
# PLUGIN_SECRET = os.environ['PLUGIN_SECRET']
PLUGIN_SECRET = "a3e0fb67-dd54-4395-82d3-393346c19807"

def callback_function(**data):
    # Extract input parameters
    project_id = data.get("projectId")
    json_export = data.get("jsonExport")
    logger = data.get("logger")
    config_str = data.get("configJSON")
    config = json.loads(config_str)

    logger.info("Plugin session is started!")

    # Check config input
    separator_char = "-"
    if "separator-character" in config:
        if isinstance(config["separator-character"], str):
            separator_char = config["separator-character"]

    # Convert annotation data to intended format
    file_list = []
    for image_index, asset in enumerate(json_export):
        external_id = asset["externalId"]
        data_url = asset["asset"]
        objects = asset["tasks"][0]["objects"]

        object_list = []
        for obj in objects:
            if "bounding-box" in obj:
                class_name = obj["title"]
                x, y = int(round(obj["bounding-box"]["x"])), int(
                    round(obj["bounding-box"]["y"])
                )
                w, h = int(round(obj["bounding-box"]["width"])), int(
                    round(obj["bounding-box"]["height"])
                )

                single_object_string = " ".join(
                    [class_name, str(x), str(y), str(w), str(h)]
                )
                object_list.append(single_object_string)
        object_string = separator_char.join(object_list)
        file_list.append(
            {"externalId": external_id, "URL": data_url, "Annotations": object_string}
        )

    # Create zip file
    zip_file_name = project_id + ".zip"
    zip_data = BytesIO()
    with zipfile.ZipFile(zip_data, mode="w") as zf:
        zf.writestr(project_id + ".json", json.dumps(file_list, indent=4))

    logger.info("Plugin session is ended!")
    return ExportResponse(
        file=zip_data,
        file_name=zip_file_name,
        storage_id="",
        bucket=""
    )

if __name__ == "__main__":
    plugin = ExportPlugin(
        id=PLUGIN_ID,
        secret=PLUGIN_SECRET,
        callback=callback_function,
        host=HOST,
        version="v3",
    )
    run(plugin, host=HOST)