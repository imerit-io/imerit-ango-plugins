import json
import os
import uuid
import copy


def alcon_conversions_second(**data):
    project_id = data.get("projectId")

    # Get json export from data response
    json_export = data.get("jsonExport")

    # get logger from data response
    logger = data.get("logger")

    # log message example
    logger.info(f"running alcon_conversions_second script on Project: {project_id}")

    # create output folder if it doesn't exist
    output_folder = os.getcwd() + f"/{project_id}"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for asset in json_export:        
        external_id = asset["externalId"]
            
        new_ann = {}

        toric_points = {
            "1": [1, 1],
            "2": [1, 1],
            "3": [1, 1],
            "4": [1, 1],
            "5": [1, 1],
            "6": [1, 1],
        }
        for ann in asset["task"]["tools"]:
            # adjust polygon anns if necessary
            if "segmentation" in ann:
                for zone in ann["segmentation"]["zones"]:
                    polygon = zone["region"]
                    if polygon[0] != polygon[-1]:
                        polygon.append(polygon[0])
                    new_ann[ann["title"]] = polygon

            elif ann.get("title") == "Keypoint_pairs":
                try:
                    answer = ann["classifications"][0]["answer"]
                except IndexError:
                    # Generate a UUID
                    generated_uuid = uuid.uuid4()

                    # Convert UUID to a hexadecimal string
                    uuid_hex = generated_uuid.hex

                    # Truncate to 4 digits
                    answer = uuid_hex[:4]
                if "Keypoint_pairs" not in new_ann:
                    new_ann["Keypoint_pairs"] = {}
                new_ann[ann["title"]][answer] = ann["point"]
            elif ann.get("title") == "Toric_dots":
                answer = ann["classifications"][0]["answer"]
                if "Toric_dots" not in new_ann:
                    new_ann["Toric_dots"] = {}
                toric_points[answer] = ann["point"]

        for key, value in toric_points.items():
            if "Toric_dots" not in new_ann:
                new_ann["Toric_dots"] = {}
            new_ann["Toric_dots"][key] = value

        with open(
            f"{output_folder}/{external_id.split('.')[0]}.json",
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(new_ann, f, indent=4)
            
    logger.info("script completed. zipping output")
    return output_folder

if "name" == "__main__":
    alcon_conversions_second()
