import json
import os
import uuid
import boto3
from PIL import Image
from io import BytesIO


def extend(**data):
    # get logger from data response
    logger = data.get("logger")
    logger.info("testing...")

    # get project id from data response
    project_id = data.get("projectId")

    # Get json export from data response
    json_export = data.get("jsonExport")

    # log message example
    logger.info(f"running script on Project: {project_id}")
    logger.info(f"this may take a few minutes...")

    # create output folder if it doesn't exist
    output_folder = os.getcwd() + f"/{project_id}"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for asset in json_export:
        batch = asset["batches"][0]
        asset_filename = asset["externalId"]
        width, height = AWS.get_image_dimensions(batch, asset_filename, logger)
        darwin_json = {
            "dataset": batch,
            "image": {
                "filename": asset["externalId"],
                "height": height,
                "original_filename": asset["externalId"],
                "path": "/",
                "width": width,
            },
            "annotations": [],
        }

        # iterate through ango annotations
        for anns in asset["task"]["tools"]:
            if anns.get("segmentation", {}).get("zones"):
                for zone in anns["segmentation"]["zones"]:
                    new_annotation = {
                        "bounding_box": {
                            "h": None,
                            "w": None,
                            "x": None,
                            "y": None,
                        },
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
                print("ELSE")
                continue

        slab_type = {
            "attributes": [],
            "id": None,
            "name": None,
            "tag": {},
        }
        for tag in asset["task"]["classifications"]:
            if tag["title"] == "Tasks":
                for answer in tag["answer"]:
                    if len(answer.split(" ")) > 1:
                        left = answer.split(" ")[0].lower()
                        right = "_" + answer.split(" ")[-1]
                    else:
                        left = answer.lower()
                        right = ""
                    darwin_json["annotations"].append(
                        {
                            "id": str(uuid.uuid4()),
                            "name": f"{left}{right}",
                            "tag": {},
                        }
                    )
            if tag["title"] == "Finish":
                slab_type["attributes"].append(tag["answer"][0])
            if tag["title"] == "Slab Type":
                slab_type["id"] = str(uuid.uuid4())
                slab_type["name"] = tag["answer"][0]
        if slab_type["attributes"] or slab_type["name"] is not None:
            darwin_json["annotations"].append(slab_type)

        filename = asset["externalId"].split("/")[-1].split(".")[0]

        # write the json file for the asset
        with open(f"{output_folder}/{filename}.json", "w", encoding="utf-8") as writer:
            json.dump(darwin_json, writer, indent=4)
    # the only thing we need to return is the output folder path
    return output_folder


class AWS:
    def get_image_dimensions(batch, filename, logger):
        bucket_name = "imerit-extendai-v7"
        access_key = os.environ.get("extend_key")
        secret_key = os.environ.get("extend_secret")
        
        s3 = boto3.client(
            "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
        )

        prefix = "input/" + batch + "/" + filename

        try:
            response = s3.get_object(
                Bucket=bucket_name, Key=prefix, Range="bytes=0-20000"
            )
            # Use BytesIO to mimic a file
            with BytesIO(response["Body"].read()) as f:
                img = Image.open(f)
                width, height = img.size
            return height, width
        except s3.exceptions.NoSuchKey:
            return 0, 0

if __name__ == "__main__":
    extend()
