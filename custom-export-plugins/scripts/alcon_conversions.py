import json
import os
import uuid
import copy


def alcon_conversions(**data):
    project_id = data.get("projectId")

    # Get json export from data response
    json_export = data.get("jsonExport")

    # get logger from data response
    logger = data.get("logger")

    # log message example
    logger.info(f"running alcon_conversions script on Project: {project_id}")

    # create output folder if it doesn't exist
    output_folder = os.getcwd() + f"/{project_id}"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    new_collection = []
    asset_counter = 1
    for asset in json_export:
        logger.info(f"Asset: {asset_counter}")
        # Get file extension
        file_extension = asset["externalId"].rsplit(".")[1]

        # get left and right external id before removing w and h
        left_external_id = asset["externalId"].split("+")[0]
        right_external_id = asset["externalId"].split("+")[1].split(".")[0]

        # filenames for output later
        left_output_filename = left_external_id.split("$")[0] + "." + file_extension
        right_output_filename = right_external_id.split("$")[0] + "." + file_extension

        # height and width of left and right images, and stitched image's total height and width
        left_width = int(left_external_id.split("$")[1].split("x")[0])
        left_height = int(left_external_id.split("$")[1].split("x")[1])
        right_width = int(right_external_id.split("$")[1].split(".")[0].split("x")[0])
        right_height = int(right_external_id.split("$")[1].split(".")[0].split("x")[1])
        try:
            total_width = asset["metadata"]["width"]
            total_height = asset["metadata"]["height"]
        except Exception as e:
            logger.info(f"{e} | {asset['assetId']}" )
            continue

        # calculate whitespace between images for conversions
        whitespace_between_images = int(total_width - right_width - left_width)

        left_ann = {}
        right_ann = {}

        # Go through annotations
        # toric_points = {
        #     "1": [1, 1],
        #     "2": [1, 1],
        #     "3": [1, 1],
        #     "4": [1, 1],
        #     "5": [1, 1],
        #     "6": [1, 1],
        # }
        for ann in asset["task"]["tools"]:
            # adjust polygon anns if necessary
            if "segmentation" in ann:
                for zone in ann["segmentation"]["zones"]:
                    polygon = zone["region"]
                    min_val = min(polygon, key=lambda x: x[0])[0]

                    # If the min value is greater than the left width,
                    # then it's on the right side
                    if min_val > left_width:  # Right side
                        for point in polygon:
                            right_min_x = left_width + whitespace_between_images
                            # if the point is less than the right min x
                            # it needs to be adjusted to the right min x
                            if point[0] < right_min_x:
                                point[0] = 0
                            else:
                                # adjust points as if left image and whitespace isnt there
                                point[0] -= right_min_x

                        # if the first and last points are not the same, append the first point to the end
                        if polygon[0] != polygon[-1]:
                            polygon.append(polygon[0])

                        right_ann[ann["title"]] = polygon
                    else:  # Left side
                        for point in polygon:
                            left_max_x = left_width
                            # if the point is greater than the left max x
                            # it needs to be adjusted to the left max x
                            if point[0] > left_max_x:
                                point[0] = left_max_x
                        # if the first and last points are not the same, append the first point to the end
                        if polygon[0] != polygon[-1]:
                            polygon.append(polygon[0])
                        left_ann[ann["title"]] = polygon
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
                if "Keypoint_pairs" not in left_ann:
                    left_ann["Keypoint_pairs"] = {}
                if "Keypoint_pairs" not in right_ann:
                    right_ann["Keypoint_pairs"] = {}
                if ann.get("point")[0] > left_width:  # right side
                    # first change x coord with out left stiched image
                    ann['point'][0] = (ann['point'][0] - (left_width + whitespace_between_images))
                    right_ann[ann["title"]][answer] = ann["point"]
                elif ann.get("point")[0] < left_width:
                    left_ann[ann["title"]][answer] = ann["point"]
            # elif ann.get("title") == "Toric_dots":
            #     answer = ann["classifications"][0]["answer"]
            #     # if "Toric_dots" not in left_ann:
            #     #     left_ann["Toric_dots"] = {}
            #     if "Toric_dots" not in right_ann:
            #         right_ann["Toric_dots"] = {}
            #     toric_points[answer] = ann["point"]

        # for key, value in toric_points.items():
        #     right_ann["Toric_dots"][key] = value

        with open(
            f"{output_folder}/{left_output_filename.split('.')[0]}.json",
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(left_ann, f)
        with open(
            f"{output_folder}/{right_output_filename.split('.')[0]}.json",
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(right_ann, f)
            
        asset_counter += 1

    logger.info("script completed. zipping output")
    return output_folder

if "name" == "__main__":
    alcon_conversions()
