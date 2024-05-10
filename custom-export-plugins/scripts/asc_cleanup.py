import json
import os
import uuid
import boto3
from PIL import Image
from io import BytesIO


def asc_cleanup(**data):
    project_id = data.get("projectId")

    # Get json export from data response
    json_export = data.get("jsonExport")

    # get logger from data response
    logger = data.get("logger")

    # log message example
    logger.info(f"running asc_cleanup script on Project: {project_id}")
    logger.info(f"this may take a few minutes...")

    # create output folder if it doesn't exist
    output_folder = os.getcwd() + f"/{project_id}"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    ango_json = []
    for asset in json_export:
        tools = asset["task"]["tools"]
        relations = asset["task"]["relations"]
        if "stageHistory" in asset["task"]:
            del asset["task"]["stageHistory"]
        for ann in tools:
            start = ann.get("ner").get("start")
            end = ann.get("ner").get("end")
            selection = ann.get("ner").get("selection")

            if selection.startswith(" ") or selection.endswith(" "):
                # Get count of removed front and back
                removed_front = len(selection) - len(selection.lstrip())
                removed_back = len(selection) - len(selection.rstrip())

                # Strip leading and trailing spaces
                modified_selection = selection.lstrip().rstrip()

                # Update start and end positions
                start += removed_front
                end -= removed_back

                # Update the dictionary
                ann["ner"]["selection"] = modified_selection
                ann["ner"]["start"] = start
                ann["ner"]["end"] = end
                # ann["ner"]["fixed"] = {
                #     "removed_front": removed_front,
                #     "removed_back": removed_back,
                #     "orig_len_selection": len(selection),
                #     "new_len_selection": len(modified_selection),
                #     "orig_selection": selection,
                #     "obj_id": ann["objectId"],
                # }
            else:
                pass
            if "stageHistory" in ann:
                del ann["stageHistory"]

        for relation in relations:
            relation["title"] = relation["title"].lstrip().rstrip()
        # Add asset to ango asset list
        ango_json.append(asset)

    with open(f"{output_folder}/{project_id}.json", "w", encoding="utf-8") as writer:
        json.dump(ango_json, writer)

    logger.info(f"script completed. zipping output")

    return output_folder


if "name" == "__main__":
    asc_cleanup()
