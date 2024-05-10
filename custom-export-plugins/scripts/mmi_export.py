import json
import glob
import os

classes = {
    "tip_1_left": {"exists": False, "id": None, "tip_ID": None, "instrument": None},
    "tip_1_right": {"exists": False, "id": None, "tip_ID": None, "instrument": None},
    "tip_2_left": {"exists": False, "id": None, "tip_ID": None, "instrument": None},
    "tip_2_right": {"exists": False, "id": None, "tip_ID": None, "instrument": None},
    "Shaft_left": {"exists": False, "id": None, "instrument": None},
    "Shaft_right": {"exists": False, "id": None, "instrument": None},
    "wrist body_left": {"exists": False, "id": None, "instrument": None},
    "wrist body_right": {"exists": False, "id": None, "instrument": None},
}
folder = "/Users/home/Downloads/cv-mmi-shared-imerit.s3.eu-west-1.amazonaws.com"
test_file = "/Users/home/Downloads/cv-mmi-shared-imerit.s3.eu-west-1.amazonaws.com/video_014.mp4.json"


def mmi_export(**data):
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
        file_name = asset["externalId"].rsplit("/", 1)[-1].split(".")[0]
        logger.info(f"{asset.keys()}")
        logger.info(f"Processing asset: {asset['externalId']}")

        new_json = asset.copy()
        # new_json = {
        #     "externalId": "video_014.mp4",
        #     "objects": [],
        # }
        new_tools = []

        for tool in asset.get("task").get("tools"):
            tool_name = tool.get("title")
            tool_obj_id = tool.get("objectId")
            tool_class = None
            instrument = None
            tip_ID = None
            if tool_name == "tip" and tool.get("segmentation") is not None:
                for _class in tool.get("classifications"):
                    if _class.get("title") == "tip_ID":
                        number = _class.get("answer")
                        tip_ID = _class.get("objectId")
                    elif _class.get("title") == "instrument":
                        side = _class.get("answer")
                        instrument = _class.get("objectId")
                tool_class = f"{tool_name}_{number}_{side}"
                if (
                    tool_class is not None
                    and classes.get(tool_class).get("exists") is False
                ):
                    classes[tool_class]["exists"] = True
                    classes[tool_class]["id"] = tool_obj_id
                    classes[tool_class]["tip_ID"] = tip_ID
                    classes[tool_class]["instrument"] = instrument
            elif tool_name != "tip" and not tool.get("interpolationStopped"):
                try:
                    side = tool.get("classifications")[0].get("answer")
                    instrument = tool.get("classifications")[0].get("objectId")
                    tool_class = f"{tool_name}_{side}"
                    if (
                        tool_class is not None
                        and classes.get(tool_class).get("exists") is False
                    ):
                        classes[tool_class]["exists"] = True
                        classes[tool_class]["id"] = tool_obj_id
                        classes[tool_class]["instrument"] = instrument
                except IndexError:
                    print(tool)
                    break

            if not tool.get("interpolationStopped"):
                new_tools.append(Helper.make_new_obj(tool, tool_class))

        new_json["task"]["tools"] = new_tools

        with open(f"{output_folder}/{file_name}.json", "w") as f:
            json.dump(new_json, f, indent=2)

    return output_folder


class Helper:
    def make_new_obj(tool, tool_class):
        new_obj = tool.copy()
        new_obj["objectId"] = classes[tool_class]["id"]
        if len(new_obj.get("classifications")) == 1:
            new_obj["classifications"][0]["objectId"] = classes[tool_class][
                "instrument"
            ]
        else:
            for idx, _class in enumerate(tool.get("classifications")):
                if _class.get("title") == "instrument":
                    new_obj["classifications"][idx]["objectId"] = classes[tool_class][
                        "instrument"
                    ]
                elif _class.get("title") == "tip_ID":
                    new_obj["classifications"][idx]["objectId"] = classes[tool_class][
                        "tip_ID"
                    ]
        return new_obj


if "name" == "__main__":
    mmi_export()
