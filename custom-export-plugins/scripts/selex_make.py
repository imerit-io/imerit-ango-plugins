import json
import os
import uuid
import hashlib


def selex_make(**data):
    # get project id from data response
    project_id = data.get("projectId")

    # get logger from data response
    logger = data.get("logger")

    # log message example
    logger.info(f"running selex make script {project_id}")

    # get config from data response if you need to get any config values
    config_str = data.get("configJSON")
    config = json.loads(config_str)

    # get json export from data response, this is a list of dictionaries, unfiltered
    json_export = data.get("jsonExport")

    # create output folder if it doesn't exist
    output_folder = os.getcwd() + f"/{project_id}"

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for asset in json_export:
        image_name = asset["externalId"]
        batch = "batch000"
        if len(asset["batches"]) >= 1:
            batch = asset["batches"][0]

        if not os.path.exists(f"{output_folder}/{batch}"):
            os.makedirs(f"{output_folder}/{batch}")

        new_filename = image_name.rsplit(".", 1)[0]
        task = asset["task"]
        labeler = task["updatedBy"]
        labeler_id = hashlib.md5(labeler.encode()).hexdigest()

        output_dict = {
            "annotation": {
                "filename": image_name,
                "labeler_id": labeler_id,
                "attributeid": "None",
                "cameraid": "None",
                "object": [],
            }
        }
        if task["classifications"] != []:
            for _class in task["classifications"]:
                title = _class["title"]
                make = _class["answer"]
                output_dict["annotation"]["object"].append({title: make})
        else:
            logger.info(f"no classifications for {image_name}")
            pass

        # write the json file if it is valid, otherwise skip to next asset
        with open(
            f"{output_folder}/{batch}/{new_filename}.json", "w", encoding="utf-8"
        ) as output:
            json.dump(output_dict, output, indent=4)

    return output_folder
