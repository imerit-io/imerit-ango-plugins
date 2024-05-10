import json
import os


def hyundai(**data):
    logger = data.get("logger")
    project_id = data.get("projectId")
    config_str = data.get("configJSON")
    config = json.loads(config_str)
    
    try:
        batch = config["batch"]
        logger.info(f"batch: {batch}")
        _temp_unfiltered_json = data.get("jsonExport")
        json_export = [asset for asset in _temp_unfiltered_json if batch in asset["batches"]] \
            if batch != "" \
            else _temp_unfiltered_json

    except KeyError:
        # if not batch, get json export from data response, this is a list of dictionaries, unfiltered
        json_export = data.get("jsonExport")
    
    logger.info("running hyundai script")
    output_folder = os.getcwd() + f"/{project_id}"

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for asset in json_export:
        file_name = asset["externalId"].split(".")[0]
        segmentation_array = []
        objects = asset["tasks"][0]["objects"]
        for obj in objects:
            for zone in obj["segmentation"]["zones"]:
                segmentation_array.append(
                    {
                        "group": "vision_fail_safety",
                        "class": obj["title"],
                        "points": zone["region"],
                    }
                )
            output_json = {
                "image_width": 1280,
                "image_height": 944,
                "vision_fail_safety_list": segmentation_array,
            }
            with open(f"{output_folder}/{file_name}.json", "w") as output:
                json.dump(output_json, output, indent=4)

    return output_folder
