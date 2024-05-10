import json
import os
import uuid
import boto3
from PIL import Image
from io import BytesIO


def feature_point_joulea(**data):
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
        darwin_json["item"]["name"] = asset["externalId"].split("+")[0] + ".JPG"
        darwin_json["item"]["path"] = "/"

        # darwin_json["item"]["slots"] = [
        #     {
        #         "type": "image",
        #         "slot_name": "slot1",
        #         "width": 640,
        #         "height": 512,
        #         "source_files": [
        #             {"file_name": f"{asset['externalId'].split('+')[0]}.JPG"}
        #         ],
        #     },
        #     {
        #         "type": "image",
        #         "slot_name": "slot2",
        #         "width": 5184,
        #         "height": 3888,
        #         "source_files": [{"file_name": f"{asset['externalId'].split('+')[1]}"}],
        #     },
        # ]

        # iterate through ango annotations
        max_thermal_width = 640
        for anns in asset["task"]["tools"]:
            try:
                attributes = anns["classifications"][0]["answer"]
            except Exception:
                logger.info(f"missing attributes classification: {anns['objectId']}")
                attributes = None

            try:
                title = anns["title"]
            except Exception:
                logger.info(f"missing title: {anns['objectId']}")
                title = None

            new_annotation = {
                "attributes": [attributes],
                "id": str(uuid.uuid4()),
                "keypoint": {"x": None, "y": None},
                "name": "Keypoint",
                # "slot_names": [],
                "filename": "",
            }

            if anns["title"] == "Thermal":
                try:
                    new_annotation["keypoint"]["x"] = anns["point"][0]
                    new_annotation["keypoint"]["y"] = anns["point"][1]
                    # new_annotation["slot_names"].append("slot1")
                    new_annotation[
                        "filename"
                    ] = f"{asset['externalId'].split('+')[0]}.JPG"
                except Exception:
                    logger.info(f"missing point: {anns['objectId']}")
            elif anns["title"] == "RGB":
                try:
                    new_annotation["keypoint"]["x"] = (
                        anns["point"][0] - max_thermal_width
                    ) * 7
                    new_annotation["keypoint"]["y"] = anns["point"][1] * 7
                    # new_annotation["slot_names"].append("slot2")
                    new_annotation["filename"] = f"{asset['externalId'].split('+')[1]}"
                except Exception:
                    logger.info(f"missing point: {anns['objectId']}")

            darwin_json["annotations"].append(new_annotation)

        try:
            filename = asset["externalId"].split("+")[0]
        except Exception:
            logger.info(f"missing filename: {asset['externalId']}")

        # write the json file for the asset
        with open(f"{output_folder}/{filename}.json", "w", encoding="utf-8") as writer:
            json.dump(darwin_json, writer, indent=4)

    logger.info(f"script completed. zipping output")
    # the only thing we need to return is the output folder path
    return output_folder


if "name" == "__main__":
    feature_point_joulea()
