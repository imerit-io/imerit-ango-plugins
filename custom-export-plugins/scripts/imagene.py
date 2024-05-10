import json
import uuid
import os


def imagene(**data):
    # get project id from data response
    project_id = data.get("projectId")

    # Get json export from data response
    json_export = data.get("jsonExport")

    # get logger from data response
    logger = data.get("logger")

    logger.info(f"running script on Project: {project_id}")

    # create output folder if it doesn't exist
    output_folder = os.getcwd() + f"/{project_id}"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    colors = {
        "Atypical": [255, 101, 255],
        "Dead": [50, 50, 50],
        "Inflammatory": [255, 179, 102],
        "Connective": [0, 255, 0],
        "Neoplastic": [51, 77, 179],
        "Epithelial": [255, 0, 0],
        "Unlabeled Pre Ann": [0, 0, 0]
    }
    for asset in json_export:
        try:
            filename = asset["externalId"].split(".")[0].split(":")[1]
            batch = asset["externalId"].split(".")[0].split(":")[0]
        except Exception as e:
            filename = asset["externalId"].split(".")[0]
            batch = asset["batches"][0]
        output_obj = {"type": "FeatureCollection", "features": []}
        for anno in asset["task"]["tools"]:
            feature = {
                "type": "Feature",
                "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, anno["objectId"])),
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [anno["polygon"]],
                },
                "properties": {
                    "objectType": "annotation",
                    "classification": {
                        "name": anno["title"],
                        "color": colors[anno["title"]],
                    },
                },
            }
            output_obj["features"].append(feature)
        if not os.path.exists(f"{output_folder}/{batch}"):
            os.makedirs(f"{output_folder}/{batch}")
        with open(
            f"{output_folder}/{batch}/{filename}.geojson",
            "w",
            encoding="utf-8",
        ) as outfile:
            json.dump(output_obj, outfile)
    return output_folder


if "name" == "__main__":
    imagene()
