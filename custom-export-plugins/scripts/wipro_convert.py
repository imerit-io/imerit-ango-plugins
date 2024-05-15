import json
import os

def wipro_convert(**data):
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

    try:
        for asset in json_export:
            filename = asset["externalId"].split(".")[0]
            new_json = {
                'imgHeight':asset.get("metadata").get("height"),
                'imgWidth':asset.get("metadata").get("width"),
                'objects':[]
            }
            
            for ann in asset["tools"]:
                title = ann.get("title")
                object_id = ann.get("objectId")
                # handle sem seg annotations
                if ann.get("segmentation"):
                    for seg in ann.get("segmentation").get("zones"):
                        polygon = seg.get("region")
                        new_json["objects"].append({
                        "label":title,
                        "polygon":polygon,
                        "OBJIDTEST": object_id
                        })
                # handle bbox annotations
                elif ann.get("bounding-box"):
                    height = ann.get("bounding-box").get("height")
                    width = ann.get("bounding-box").get("width")
                    min_x = ann.get("bounding-box").get("x")
                    min_y = ann.get("bounding-box").get("y")
                    max_x = min_x + width
                    max_y = min_y + height
                    new_json["objects"].append({
                        "label":title,
                        "polygon":[[min_x, min_y], [max_x, min_y], [max_x, max_y], [min_x, max_y]],
                        "OBJIDTEST": object_id
                    })
                
            # write new json to file
            with open(f"{output_folder}/{filename}.json", "w", encoding="utc-8") as f:
                json.dump(new_json, f, indent=2)
    except Exception as e:
        logger.error(f"Error converting json: {e}")
        
if "name" == "__main__":
    wipro_convert()