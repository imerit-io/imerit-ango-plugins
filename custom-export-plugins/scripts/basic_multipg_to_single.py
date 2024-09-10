import json
import os
import uuid
import hashlib

def basic_multipg_to_single(**data):
    # get project id from data response
    project_id = data.get("projectId")

    # get logger from data response
    logger = data.get("logger")

    # log message example
    logger.info(f"running basic_multipg_to_single script {project_id}")

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
        collector = {}
        for annotation in asset["task"]["tools"]:
            dataset = asset["dataset"]
            dataset_reference = dataset[annotation["page"]]
            external_id = dataset_reference.split("?")[0].split("amazonaws.com/")[1]
            sub_folders = "/".join(external_id.split("/")[0:-1])
            if not os.path.exists(f"{output_folder}/{sub_folders}"):
                os.makedirs(f"{output_folder}/{sub_folders}", exist_ok=True)
            if annotation["page"] not in collector:
                collector[annotation["page"]] = {
                    "externalId": external_id,
                    "assetId": asset["assetId"],
                    "totalPages": len(dataset),
                    "tools": []
               }
            new_annotation = annotation.copy()
            del new_annotation["page"]
            del new_annotation["metadata"]
            new_annotation["page"] = annotation["page"] + 1
            collector[annotation["page"]]["tools"].append(new_annotation)
        for images in collector.values():
            filename = images["externalId"] + ".json"
            with open(f"{output_folder}/{filename}", "w") as output:
                json.dump(images, output, indent=4)
                
    return output_folder