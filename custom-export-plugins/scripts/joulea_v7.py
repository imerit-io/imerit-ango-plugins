import json
import os
import uuid
import boto3
from PIL import Image
from io import BytesIO


def joulea(**data):
    # get project id from data response
    project_id = data.get("projectId")

    # Get json export from data response
    json_export = data.get("jsonExport")

    # get logger from data response
    logger = data.get("logger")

    # log message example
    logger.info(f"running script on Project: {project_id}")
    logger.info(f"this may take a few minutes...")

    # create output folder if it doesn't exist
    output_folder = os.getcwd() + f"/{project_id}"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for asset in json_export:
        batch = asset["batches"][0]
        darwin_json = {
            "version": "2.0",
            "schema_ref": "https://darwin-public.s3.eu-west-1.amazonaws.com/darwin_json_2_0.schema.json",
            "item": {},
            "annotations": [],
        }
        # darwin_json["item"]["name"] = asset["externalId"]
        darwin_json["item"]["name"] = asset['externalId'].split("+")[0] + ".JPG"
        darwin_json["item"]["path"] = "/"

        darwin_json["item"]["slots"] = []

        # iterate through ango annotations
        for anns in asset["task"]["tools"]:
            if anns.get("segmentation", {}).get("zones"):
                for zone in anns["segmentation"]["zones"]:
                    new_annotation = {
                        "bounding_box": {"h": None, "w": None, "x": None, "y": None},
                        "id": None,
                        "name": None,
                        "polygon": {"paths": []},
                    }
                    new_annotation["id"] = str(uuid.uuid4())
                    new_annotation["name"] = anns["title"]
                    max_x = float("-inf")  # default num
                    max_y = float("-inf")  # default num
                    min_x = float("inf")  # default num
                    min_y = float("inf")  # default num
                    path_array = []
                    # for points in anns["segmentation"]["zones"][0]["region"]:
                    for points in zone["region"]:
                        if points[0] > max_x:
                            max_x = points[0]
                        if points[0] < min_x:
                            min_x = points[0]
                        if points[1] > max_y:
                            max_y = points[1]
                        if points[1] < min_y:
                            min_y = points[1]
                        path_array.append({"x": points[0], "y": points[1]})

                    if zone.get("holes"):
                        for ango_holes in zone["holes"]:
                            hole_arr = []
                            for hole in ango_holes:
                                hole_dict = {"x": hole[0], "y": hole[1]}
                                hole_arr.append(hole_dict)
                            new_annotation["polygon"]["paths"].append(hole_arr)

                    new_annotation["polygon"]["paths"].append(path_array)
                    new_annotation["bounding_box"]["h"] = max_y - min_y
                    new_annotation["bounding_box"]["w"] = max_x - min_x
                    new_annotation["bounding_box"]["x"] = min_x
                    new_annotation["bounding_box"]["y"] = min_y

                    darwin_json["annotations"].append(new_annotation)
            elif anns.get("bounding-box", {}):
                new_annotation = {
                    "bounding_box": {
                        "h": anns["bounding-box"]["height"],
                        "w": anns["bounding-box"]["width"],
                        "x": anns["bounding-box"]["x"],
                        "y": anns["bounding-box"]["y"],
                    },
                    "id": str(uuid.uuid4()),
                    "name": anns["title"],
                }
                darwin_json["annotations"].append(new_annotation)
            else:
                continue

        for tag in asset["task"]["classifications"]:
            darwin_json["annotations"].append(
                {
                    "id": str(uuid.uuid4()),
                    "name": tag["answer"][0],
                    "slot_names": ["0"],
                    "tag": {},
                }
            )

        filename = asset["externalId"].split("/")[-1].split(".")[0]

        # write the json file for the asset
        with open(f"{output_folder}/{filename}.json", "w", encoding="utf-8") as writer:
            json.dump(darwin_json, writer, indent=4)
    logger.info(f"script completed. zipping output")
    # the only thing we need to return is the output folder path
    return output_folder


if "name" == "__main__":
    joulea()
