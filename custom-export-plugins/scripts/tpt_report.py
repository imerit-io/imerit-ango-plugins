import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
from pydantic import BaseModel
import os


def tpt_report(**data):
    ango_data = data.get("jsonExport")
    project_id = data.get("projectId")
    logger = data.get("logger")
    logger.info(f"running tpt_report {project_id}")
    if len(data.get('stage')) == 0: # Stage is a list, no stage selected returns []
        logger.error("stage is []")
        raise ValueError("You must choose a Stage Filter to run this plugin.")

    df = Aggregate().metrics(ango_data)

    output_folder = os.getcwd() + f"/{project_id}"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_path = f"{output_folder}/tpt_report_{project_id}.csv"
    df.to_csv(output_path, index=False)

    return output_folder


class TptReport(BaseModel):
    batch: Optional[str] = None
    file_id: Optional[str] = None
    status: Optional[str] = None
    outcome: Optional[str] = None
    total_time_label_qc: Optional[float] = None
    total_time_all: Optional[float] = None
    classifications: Optional[int] = None
    objects: Optional[int] = None
    relations: Optional[int] = None


class Aggregate:
    @staticmethod
    def get_stage_data(stages: List[Dict[str, Any]]) -> Dict[str, Any]:
        # This can't be replaced by taking the value from `asset['task']['totalDuration']`
        stage_name_data = {}
        stage_data = {}
        for stage in stages:
            if stage["stage"] == "Start" or stage["stage"] == "Complete":
                continue
            stage_name = stage["stage"].lower().replace(" ", "_")
            try:
                stage_name_data[stage_name] += 1
            except KeyError:
                stage_name_data[stage_name] = 0
            stage_name_index = stage_name_data[stage_name]
            stage_data[f"{stage_name}_date{stage_name_index}"] = stage[
                "completedAt"
            ].split("T")[0]
            stage_data[f"{stage_name}_duration{stage_name_index}"] = "{0:.2f}".format(
                float(stage["duration"] / 60000)
            )
            try:
                stage_data[f"{stage_name}_assignee{stage_name_index}"] = stage[
                    "completedBy"
                ]
            except KeyError:
                pass
        return stage_data

    @staticmethod
    def get_total_time_label_qc(stage_data: Dict[str, Any]) -> Optional[float]:
        for k in stage_data.keys():
            if "qc" in k:
                total_time_label_qc = 0
                for key, value in stage_data.items():
                    if "qc" or "label" in key:
                        if "duration" in key:
                            total_time_label_qc += float(value)
                return "{0:.2f}".format(total_time_label_qc)

    @staticmethod
    def get_total_time_all(stage_data: Dict[str, Any]) -> Optional[float]:
        total_time_all = 0
        for k, v in stage_data.items():
            if "duration" in k:
                total_time_all += float(v)
        if total_time_all:
            return "{0:.2f}".format(total_time_all)
        else:
            return None

    def flatten_classifications(
        classifications: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        flattened_list = []
        for item in classifications:
            flattened_item = {}
            for k, v in item.items():
                if k != "classifications":
                    flattened_item[k] = v
            flattened_list.append(flattened_item)
            if item["classifications"]:
                flattened_list.extend(
                    Aggregate.flatten_classifications(item["classifications"])
                )
        return flattened_list

    def aggregate_metrics(
        self, ango_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        annotations = []
        for asset in ango_data:
            tpt_report = TptReport()

            try:
                task = asset["task"]
            except KeyError:
                task = asset["tasks"][0]

            if "batches" in asset.keys() and asset["batches"]:
                tpt_report.batch = asset["batches"][0]

            if "externalId" in asset.keys():
                tpt_report.file_id = asset["externalId"]

            if "stage" in task.keys():
                tpt_report.status = task["stage"]

            if "classifications" in task.keys():
                flattened_classifications = Aggregate.flatten_classifications(
                    task["classifications"]
                )
                tpt_report.classifications = len(flattened_classifications)

            if "objects" in task.keys():
                tpt_report.objects = len(task["objects"])

            if "relations" in task.keys():
                tpt_report.relations = len(task["relations"])

            tpt_report_dict = tpt_report.dict()

            if "stageHistory" in task.keys():
                stages = task["stageHistory"]
                stage_data = self.get_stage_data(stages)

                try:
                    if len(stages) > 2:
                        tpt_report.outcome = stages[-1]["reviewStatus"]
                except KeyError:
                    pass
                if not tpt_report.outcome:
                    try:
                        if len(stages) > 2:
                            tpt_report.outcome = stages[-2]["reviewStatus"]
                    except KeyError:
                        pass

                tpt_report.total_time_label_qc = self.get_total_time_label_qc(
                    stage_data
                )
                if tpt_report.status == "Complete":
                    tpt_report.total_time_all = self.get_total_time_all(stage_data)
                tpt_report_dict.update(stage_data)

            annotations.append(tpt_report_dict)
        return annotations

    def metrics(self, ango_data: Path):
        annotations = self.aggregate_metrics(ango_data)
        return pd.DataFrame(annotations)
