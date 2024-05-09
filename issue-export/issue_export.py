import os
import re
import json
import zipfile
import warnings
import traceback
import numpy as np
import pandas as pd
from io import BytesIO
from datetime import datetime
from imerit_ango.sdk import SDK
from imerit_ango.plugins import ExportPlugin, ExportResponse, run

warnings.simplefilter(action='ignore', category=FutureWarning)

HOST = os.environ['HOST']
PLUGIN_ID = os.environ['PLUGIN_ID']
PLUGIN_SECRET = os.environ['PLUGIN_SECRET']


def print_session_start(logger):
    time_str = str(datetime.utcnow())
    time_str = time_str[:-3].replace(' ', 'T') + 'Z'
    time_str = time_str.replace(':', '-')

    print("[" + time_str + "][INFO]" + " Plugin session is started!")
    logger.info("Plugin session is started!")


def print_session_info(data):
    try:
        project_id = data.get('projectId')
        run_by = data.get('runBy')
        organization_id = data.get('orgId')
        config = data.get('configJSON')
        stages = data.get('stage')
        batches = data.get('batches')
        num_assets = data.get('numTasks')
        api_key = data.get("apiKey")
        sdk = SDK(api_key=api_key, host=HOST)

        sdk_response = sdk.get_project(project_id)

        stage_names = ', '.join(stages)
        project_name = ""
        batch_names = ""
        if 'data' in sdk_response:
            project_name = sdk_response['data']['project']['name']
            project_batches = sdk_response['data']['project']['batches']
            batch_name_list = []
            if batches is not None:
                for batch_id in batches:
                    for project_batch in project_batches:
                        if project_batch['_id'] == batch_id:
                            batch_name = project_batch['name']
                            batch_name_list.append(batch_name)
                            continue
                batch_names = ', '.join(batch_name_list)

        print('-' * 60)
        print('Project Name: ' + project_name)
        print('Project ID: ' + project_id)
        print('Run By: ' + run_by)
        print('Organization ID: ' + organization_id)
        print('Stages: ' + stage_names)
        print('Batches: ' + batch_names)
        print('Num Assets: ' + str(num_assets))
        print('Config: ')
        print(config)
        print('-' * 60)
    except Exception as e:
        print(e)


def print_session_end(logger):
    time_str = str(datetime.utcnow())
    time_str = time_str[:-3].replace(' ', 'T') + 'Z'
    time_str = time_str.replace(':', '-')

    print("[" + time_str + "][INFO]" + " Plugin session is ended!")
    logger.info("Plugin session is ended!")


def print_info_message(logger, message):
    time_str = str(datetime.utcnow())
    time_str = time_str[:-3].replace(' ', 'T') + 'Z'
    time_str = time_str.replace(':', '-')

    print("[" + time_str + "][INFO]" + " " + message)
    logger.info(message)


def check_config(config):
    add_assignee_info = True
    if 'add_assignee_info' in config:
        if isinstance(config['add_assignee_info'], bool):
            add_assignee_info = config['add_assignee_info']

    extract_metrics = True
    if 'extract_metrics' in config:
        if isinstance(config['extract_metrics'], bool):
            extract_metrics = config['extract_metrics']

    verbose = True
    if 'verbose' in config:
        if isinstance(config['verbose'], bool):
            verbose = config['verbose']

    verbose_frequency = 1000
    if 'verbose_frequency' in config:
        if isinstance(config['verbose_frequency'], int):
            if config['verbose_frequency'] > 0:
                verbose_frequency = config['verbose_frequency']

    return add_assignee_info, extract_metrics, verbose, verbose_frequency


def filter_stage_history(raw_stage_history, project_stage_df):
    stage_history = []
    stage_index_list = []
    stage_id_list = []
    stage_completed_at_list = []
    for stage_index, stage in enumerate(raw_stage_history):
        if stage["stageId"] not in project_stage_df["stage_id"].tolist():
            continue
        stage_index_list.append(stage_index)
        stage_id_list.append(stage["stageId"])
        stage_completed_at_list.append(stage["completedAt"])

    stage_history_df = pd.DataFrame()
    stage_history_df["stage_index"] = stage_index_list
    stage_history_df["stage_id"] = stage_id_list
    stage_history_df["stage_completed_at"] = stage_completed_at_list

    key_index_list = []
    for stage_id in project_stage_df["stage_id"].tolist():
        filtered_stage_history_df = stage_history_df[stage_history_df["stage_id"] == stage_id]
        key_index = 0
        if len(filtered_stage_history_df) == 0:
            continue
        elif len(filtered_stage_history_df) == 1:
            key_index = int(filtered_stage_history_df["stage_index"].iloc[0])
        elif len(filtered_stage_history_df) > 1:
            sorted_stage_history = filtered_stage_history_df.sort_values("stage_completed_at", ascending=False)
            sorted_stage_history = sorted_stage_history.reset_index(drop=True)
            key_index = int(sorted_stage_history["stage_index"].iloc[0])
        key_index_list.append(key_index)

    for key_index in key_index_list:
        stage_history.append(raw_stage_history[key_index])
    return stage_history


def filter_stage_history(raw_stage_history, project_stage_df):
    stage_history = []
    stage_index_list = []
    stage_id_list = []
    stage_completed_at_list = []
    for stage_index, stage in enumerate(raw_stage_history):
        if stage["stageId"] not in project_stage_df["stage_id"].tolist():
            continue
        stage_index_list.append(stage_index)
        stage_id_list.append(stage["stageId"])
        stage_completed_at_list.append(stage["completedAt"])

    stage_history_df = pd.DataFrame()
    stage_history_df["stage_index"] = stage_index_list
    stage_history_df["stage_id"] = stage_id_list
    stage_history_df["stage_completed_at"] = stage_completed_at_list

    key_index_list = []
    for stage_id in project_stage_df["stage_id"].tolist():
        filtered_stage_history_df = stage_history_df[stage_history_df["stage_id"] == stage_id]
        if len(filtered_stage_history_df) == 0:
            continue
        elif len(filtered_stage_history_df) == 1:
            key_index = int(filtered_stage_history_df["stage_index"].iloc[0])
        elif len(filtered_stage_history_df) > 1:
            sorted_stage_history = filtered_stage_history_df.sort_values("stage_completed_at", ascending=False).reset_index(drop=True)
            key_index = int(sorted_stage_history["stage_index"].iloc[0])
        key_index_list.append(key_index)

    for key_index in key_index_list:
        stage_history.append(raw_stage_history[key_index])
    return stage_history


def get_stage_statistics(ango_sdk, project_id, stage_df):
    items_per_page = 1000
    asset_id_list = []

    batch_list = []
    external_id_list = []
    assignee_list = []
    stage_name_list = []
    stage_type_list = []

    page = 1
    remaining_tasks = 1
    while remaining_tasks > 0:
        response = ango_sdk.get_assets(project_id, page=page, limit=items_per_page)
        assets = response['data']['assets']
        for asset in assets:
            asset_id = asset['_id']
            asset_id_list.append(asset_id)

            batches = asset['batches']
            batch = ', '.join(batches)
            external_id = asset['externalId']

            raw_stage_history = asset['labelTasks'][0]['stageHistory']
            stage_history = filter_stage_history(raw_stage_history, stage_df)

            for stage in stage_history:
                stage_id = stage['stageId']
                completed_by = stage['completedBy']
                stage_name = stage_df[stage_df["stage_id"] == stage_id]["stage_name"].iloc[0]
                stage_type = stage_df[stage_df["stage_id"] == stage_id]["stage_type"].iloc[0]

                batch_list.append(batch)
                external_id_list.append(external_id)
                assignee_list.append(completed_by)
                stage_name_list.append(stage_name)
                stage_type_list.append(stage_type)

        remaining_tasks = response["data"]["total"] - len(asset_id_list)
        page += 1

    data_df = pd.DataFrame()
    data_df["Batch"] = batch_list
    data_df["External ID"] = external_id_list
    data_df["Assignee"] = assignee_list
    data_df["Stage Name"] = stage_name_list
    data_df["Stage Type"] = stage_type_list

    return data_df


def callback_function(**data):
    logger = data.get('logger')

    config_str = data.get('configJSON')
    try:
        config = json.loads(config_str)
    except Exception as error:
        print(error)
        message = 'The config JSON format is wrong!'
        print_info_message(logger, message)
        message = 'Default parameters are used.'
        print_info_message(logger, message)
        config = {}

    mode = 'default'
    if 'mode' in config:
        if isinstance(config['mode'], str):
            mode = config['mode']

    try:
        if mode.lower() == 'default':
            return issue_export(data)
        elif mode.lower() == 'bosch':
            return issue_export_bosch(data)
        elif mode.lower() == 'bosch-test':
            return issue_export_bosch_test(data)
        else:
            return issue_export(data)
    except Exception as error_message:
        full_traceback = traceback.format_exc()
        traceback_lines = full_traceback.splitlines()
        processed_traceback_list = []
        for line in traceback_lines:
            traceback_line = re.sub(r'File ".*[\\/]([^\\/]+.py)"', r'File "\1"', line)
            processed_traceback_list.append(traceback_line)

        processed_traceback = '\n'.join(processed_traceback_list)
        print_info_message(logger, "Error!")
        print_info_message(logger, processed_traceback)
        raise Exception(error_message)


def issue_export(data):
    # Extract input parameters
    project_id = data.get('projectId')
    # json_export = data.get('jsonExport')
    # num_assets = data.get('numTasks')
    api_key = data.get("apiKey")
    logger = data.get('logger')
    config_str = data.get('configJSON')
    try:
        config = json.loads(config_str)
    except Exception as error:
        print(error)
        message = 'The config JSON format is wrong!'
        print_info_message(logger, message)
        message = 'Default parameters are used.'
        print_info_message(logger, message)
        config = {}
    # -------------------------------------------------
    print_session_start(logger)
    print_session_info(data)
    add_assignee_info, extract_metrics, verbose, verbose_frequency = check_config(config)

    sdk = SDK(api_key=api_key, host=HOST)

    if extract_metrics:
        # Get project ontology
        sdk_response = sdk.get_project(project_id)
        project_ontology = sdk_response['data']['project']['categorySchema']

        # Get stages
        stages = sdk_response['data']['project']['stages']
        project_stage_names = []
        project_stage_ids = []
        project_stage_types = []
        project_stage_x_position = []
        for stage in stages:
            if stage['name'] in ['Start', 'Complete']:
                continue
            project_stage_names.append(stage['name'])
            project_stage_ids.append(stage['id'])
            project_stage_types.append(stage['type'])
            # Find position of the stage
            x_position = 0
            x_position_found = False
            if "position" in stage:
                if "x" in stage["position"]:
                    x_position = stage['position']['x']
                    x_position_found = True

            if not x_position_found:
                if "consensusId" in stage:
                    consensus_id = stage["consensusId"]
                    for search_stage in stages:
                        if search_stage["type"] == "Consensus":
                            if search_stage["id"] == consensus_id:
                                x_position = search_stage['position']['x']
                                x_position_found = True
            project_stage_x_position.append(x_position)

        stage_df = pd.DataFrame()
        stage_df["stage_id"] = project_stage_ids
        stage_df["stage_name"] = project_stage_names
        stage_df["stage_type"] = project_stage_types
        stage_df["x_position"] = project_stage_x_position

        stage_df = stage_df.sort_values(by=["x_position", "stage_name"], ascending=[True, True])
        stage_df = stage_df.reset_index(drop=True)
        stage_df = stage_df.drop(columns=["x_position"])

        asset_stat_df = get_stage_statistics(sdk, project_id, stage_df)

        annotator_level_asset_stat = asset_stat_df[["Assignee", "Stage Name"]].value_counts()
        annotator_level_asset_stat = annotator_level_asset_stat.reset_index()

        batch_level_asset_stat = asset_stat_df[["Batch", "Assignee", "Stage Name"]].value_counts()
        batch_level_asset_stat = batch_level_asset_stat.reset_index()

    issue_list = sdk.exportV3(project_id, export_type="issue")
    num_issues = len(issue_list)

    print_info_message(logger, "The number of exported issues: " + str(num_issues))
    if num_issues == 0:
        raise Exception("Empty export exception!")

    # Get project ontology
    sdk_response = sdk.get_project(project_id)
    project_name = sdk_response['data']['project']['name']

    # Get stages
    stages = sdk_response['data']['project']['stages']
    project_stage_names = []
    project_stage_ids = []
    project_stage_types = []
    project_stage_x_position = []
    for current_stage in stages:
        if current_stage['name'] in ['Start', 'Complete']:
            continue
        if current_stage['type'] not in ["Label", "Review"]:
            continue
        project_stage_names.append(current_stage['name'])
        project_stage_ids.append(current_stage['id'])
        project_stage_types.append(current_stage['type'])
        # Find position of the stage
        x_position = 0
        x_position_found = False
        if "position" in current_stage:
            if "x" in current_stage["position"]:
                x_position = current_stage['position']['x']
                x_position_found = True

        if not x_position_found:
            if "consensusId" in current_stage:
                consensus_id = current_stage["consensusId"]
                for search_stage in stages:
                    if search_stage["type"] == "Consensus":
                        if search_stage["id"] == consensus_id:
                            x_position = search_stage['position']['x']
                            x_position_found = True
        project_stage_x_position.append(x_position)

    stage_df = pd.DataFrame()
    stage_df["stage_id"] = project_stage_ids
    stage_df["stage_name"] = project_stage_names
    stage_df["stage_type"] = project_stage_types
    stage_df["x_position"] = project_stage_x_position

    stage_df = stage_df.sort_values(by=["x_position", "stage_name"], ascending=[True, True])
    stage_df = stage_df.reset_index(drop=True)
    stage_df = stage_df.drop(columns=["x_position"])

    assignee_lists = []
    for index in range(len(stage_df)):
        assignee_lists.append([])

    batch_name_list = []
    external_id_list = []
    issue_id_list = []
    issue_link_list = []
    stage_list = []
    created_by_list = []
    created_at_date_list = []
    created_at_time_list = []
    status_list = []
    error_type_list = []
    error_code_list = []
    content_list = []
    comments_list = []
    position_list = []
    page_list = []
    page_end_list = []
    for issue_index, issue in enumerate(issue_list):
        if verbose & (issue_index % verbose_frequency == 0):
            logger.info("Issues Processed: [" + str(num_issues) + "/" + str(issue_index) + "]")
        # --------------------------------------------------------------------------------------------------------------
        batches = issue["asset"]["batches"]
        batches_str = ""
        try:
            batches_str = ", ".join(batches)
        except Exception as err:
            print(err)
        external_id = issue["asset"]["externalId"]

        stage = ""
        if "stage" in issue:
            stage = issue["stage"]
        elif "stageId" in issue:
            stage = issue["stageId"]

        issue_id = issue["_id"]
        label_task = issue["labelTask"]

        issue_link = "https://imerit.ango.ai/issue/" + label_task  + "?issue=" + issue_id

        created_by = issue["createdBy"]
        created_at = issue["createdAt"]

        page = ""
        if "page" in issue:
            page_value = int(issue["page"]) + 1
            page = str(page_value)

        page_end = ""
        if "pageEnd" in issue:
            page_end = str(issue["pageEnd"])

        # Calculate Time Fields
        time_split = created_at.split('T')
        created_date = time_split[0]
        created_time = time_split[1][0:8]

        year, month, day = created_date.split('-')
        hour, minute, second = created_time.split(':')

        created_datetime = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))

        create_date = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2)
        create_time = str(created_datetime.hour).zfill(2) + ':' + str(created_datetime.minute).zfill(2) + ':' + str(created_datetime.second).zfill(2)

        status = issue["status"]
        error_type = ""
        if "errorType" in issue:
            error_type = issue["errorType"]
        error_code = ""
        if "errorCode" in issue:
            error_code = issue["errorCode"]
        content = issue["content"]
        comments = issue["comments"]

        comments_str_list = []
        for comment in comments:
            comments_str_list.append(comment["createdBy"] + ": " + comment["content"])
        comments_str = "\n".join(comments_str_list)

        position = ""
        if "position" in issue:
            position_var = issue["position"]
            position_var = position_var.replace("[", "")
            position_var = position_var.replace("]", "")
            coordinates_str = position_var.split(',')
            coordinates_processed = []
            for coordinate in coordinates_str:
                coordinate_num = float(coordinate)
                coordinate_num_rounded = str(round(coordinate_num, 2))
                coordinates_processed.append(coordinate_num_rounded)
            position = ", ".join(coordinates_processed)

        batch_name_list.append(batches_str)
        external_id_list.append(external_id)
        issue_id_list.append(issue_id)
        issue_link_list.append(issue_link)
        stage_list.append(stage)
        created_by_list.append(created_by)
        created_at_date_list.append(create_date)
        created_at_time_list.append(create_time)
        status_list.append(status)
        error_type_list.append(error_type)
        error_code_list.append(error_code)
        content_list.append(content)
        comments_list.append(comments_str)
        position_list.append(position)
        page_list.append(page)
        page_end_list.append(page_end)

    page_flag = True
    unique_page_list = list(set(page_list))
    if len(unique_page_list) == 1:
        if unique_page_list[0] == "":
            page_flag = False

    page_end_flag = True
    unique_page_end_list = list(set(page_end_list))
    if len(unique_page_end_list) == 1:
        if unique_page_end_list[0] == "":
            page_end_flag = False

    # Get reviewers and annotators for each unique external id
    if add_assignee_info:
        unique_external_ids = list(set(external_id_list))

        unique_assignee_lists = []
        for index in range(len(stage_df)):
            unique_assignee_lists.append([])

        for external_id in unique_external_ids:
            retrieved_asset = sdk.get_assets(project_id=project_id, external_id=external_id)
            stage_history_raw = retrieved_asset["data"]["assets"][0]["labelTasks"][0]["stageHistory"]
            stage_history = filter_stage_history(stage_history_raw, stage_df)

            for stage_index in range(len(stage_df)):
                search_stage_id = stage_df.iloc[stage_index]["stage_id"]

                stage_assignee = ""
                for current_stage in stage_history:
                    if search_stage_id == current_stage["stageId"]:
                        stage_assignee = current_stage["completedBy"]
                        break
                unique_assignee_lists[stage_index].append(stage_assignee)

        # Fill assignee_lists using unique_assignee_lists
        for external_id in external_id_list:
            search_index = unique_external_ids.index(external_id)
            for stage_index in range(len(stage_df)):
                current_item = unique_assignee_lists[stage_index][search_index]
                assignee_lists[stage_index].append(current_item)

    data_df = pd.DataFrame()
    data_df['Batch'] = batch_name_list
    data_df['External ID'] = external_id_list
    data_df['Issue ID'] = issue_id_list
    data_df['Issue Link'] = issue_link_list

    list_of_assignee_columns = []
    if add_assignee_info:
        for stage_index in range(len(stage_df)):
            stage_name = stage_df.iloc[stage_index]["stage_name"]
            stage_type = stage_df.iloc[stage_index]["stage_type"]
            assignee_list = assignee_lists[stage_index]
            if stage_type == "Label":
                column_name = "Labeler [" + stage_name + "]"
            elif stage_type == "Review":
                column_name = "Reviewer [" + stage_name + "]"

            data_df[column_name] = assignee_list
            list_of_assignee_columns.append(column_name)

    data_df['Stage'] = stage_list
    data_df['Created By'] = created_by_list
    data_df['Created At - Date'] = created_at_date_list
    data_df['Created At - Time'] = created_at_time_list
    if page_flag:
        data_df['Page'] = page_list
    if page_end_flag:
        data_df['Page End'] = page_end_list
    data_df['Position'] = position_list
    data_df['Status'] = status_list
    data_df['Error Type'] = error_type_list
    data_df['Error Code'] = error_code_list
    data_df['Content'] = content_list
    data_df['Comments'] = comments_list
    data_df = data_df.sort_values(by=["Batch", "External ID", "Created At - Date", "Created At - Time"],
                                  ascending=[True, True, True, True])
    data_df = data_df.reset_index(drop=True)

    logger.info("Issues Processed: [" + str(num_issues) + "/" + str(num_issues) + "]")

    time_str = str(datetime.utcnow())
    time_str = time_str[:-3].replace(' ', 'T') + 'Z'
    time_str = time_str.replace(':', '-')

    annotator_level_metrics_folder = "AnnotatorLevelMetrics"
    batch_level_metrics_folder = "BatchLevelMetrics"

    output_filename_list = []
    if extract_metrics:
        if not os.path.exists(annotator_level_metrics_folder):
            os.mkdir(annotator_level_metrics_folder)
        if not os.path.exists(batch_level_metrics_folder):
            os.mkdir(batch_level_metrics_folder)

        for assignee_column in list_of_assignee_columns:
            stage_name = assignee_column[assignee_column.index("[")+1: assignee_column.index("]")]

            # Extract annotator level metrics
            sliced_data_df = data_df[[assignee_column, "Error Code"]]
            counts_df = sliced_data_df.value_counts().reset_index()

            pivot_df = counts_df.pivot_table(index=[assignee_column], columns=["Error Code"], values=0, aggfunc=np.sum, fill_value=0)
            pivot_df = pivot_df.reset_index()

            output_filename = project_id + '-export-' + time_str + '_[AnnotatorLevelMetrics][' + assignee_column + '].csv'
            output_filepath = os.path.join(annotator_level_metrics_folder, output_filename)
            pivot_df.to_csv(output_filepath, index=False)
            output_filename_list.append(output_filepath)

            # Extract batch level metrics
            sliced_data_df = data_df[["Batch", assignee_column, "Error Code"]]
            counts_df = sliced_data_df.value_counts().reset_index()

            pivot_df = counts_df.pivot_table(index=["Batch", assignee_column], columns=["Error Code"], values=0, aggfunc=np.sum, fill_value=0)
            pivot_df = pivot_df.reset_index()

            output_filename = project_id + '-export-' + time_str + '_[BatchLevelMetrics][' + assignee_column + '].csv'
            output_filepath = os.path.join(batch_level_metrics_folder, output_filename)
            pivot_df.to_csv(output_filepath, index=False)
            output_filename_list.append(output_filepath)

    output_filename = project_id + '-export-' + time_str + '_[Issue].csv'
    data_df.to_csv(output_filename, index=False)

    zip_filename = project_id + '-export-' + time_str + '.zip'
    mem_zip = BytesIO()
    with zipfile.ZipFile(mem_zip, mode="w") as zf:
        zf.write(output_filename)
        for current_filename in output_filename_list:
            zf.write(current_filename)

    os.remove(output_filename)
    for current_filename in output_filename_list:
        os.remove(current_filename)

    if os.path.exists(annotator_level_metrics_folder):
        os.rmdir(annotator_level_metrics_folder)
    if os.path.exists(batch_level_metrics_folder):
        os.rmdir(batch_level_metrics_folder)

    print_session_end(logger)

    export_response_obj = ExportResponse(file=mem_zip, file_name=zip_filename, storage_id="", bucket="")
    return export_response_obj


def issue_export_bosch(data):
    # Extract input parameters
    project_id = data.get('projectId')
    json_export = data.get('jsonExport')
    num_assets = data.get('numTasks')
    api_key = data.get("apiKey")
    logger = data.get('logger')
    config_str = data.get('configJSON')
    try:
        config = json.loads(config_str)
    except Exception as error:
        print(error)
        message = 'The config JSON format is wrong!'
        print_info_message(logger, message)
        message = 'Default parameters are used.'
        print_info_message(logger, message)
        config = {}
    # -------------------------------------------------
    print_session_start(logger)
    print_session_info(data)

    print_info_message(logger, "The number of exported assets: " + str(num_assets))
    if num_assets == 0:
        raise Exception("Empty export exception!")

    add_assignee_info, extract_metrics, verbose, verbose_frequency = check_config(config)

    sdk = SDK(api_key=api_key, host=HOST)

    issue_list = sdk.exportV3(project_id, export_type="issue")
    num_issues = len(issue_list)

    created_date_list = []
    external_id_list = []
    object_id_list = []
    issue_link_list = []
    created_by_list = []
    created_at_list = []
    error_type_list = []
    error_code_list = []
    page_start_list = []
    page_end_list = []
    remarks_list = []
    for issue in issue_list:
        original_external_id = issue["asset"]["externalId"]
        external_id = re.sub("[^0-9]", "", original_external_id)
        if len(external_id) == 0:
            external_id = original_external_id

        object_id = ""
        if 'objectId' in issue:
            object_id = issue['objectId']
        issue_id = issue["_id"]
        label_task = issue["labelTask"]
        issue_link = "https://imerit.ango.ai/issue/" + label_task + "?issue=" + issue_id

        created_by = issue["createdBy"]
        created_at = issue["createdAt"]

        error_type = ""
        if 'errorType' in issue:
            error_type = issue['errorType']

        error_code = ""
        if 'errorCode' in issue:
            error_code = issue['errorCode']

        page_start = 0
        if 'page' in issue:
            page_start = int(issue["page"]) + 1

        page_end = 0
        if 'pageEnd' in issue:
            page_end = int(issue["pageEnd"]) + 1

        content = ""
        if 'content' in issue:
            content = issue['content']

        comments = ""
        if 'comments' in issue:
            comments = issue['comments']

        # Calculate Time Fields
        time_split = created_at.split('T')
        created_date = time_split[0]
        created_time = time_split[1][0:8]

        year, month, day = created_date.split('-')
        hour, minute, second = created_time.split(':')

        created_datetime = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
        create_date = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2)

        # comments_str_list = []
        # comments_str_list.append(created_by + ": " + content)
        # for comment in comments:
        #     comments_str_list.append(comment["createdBy"] + ": " + comment["content"])
        # comments_str = "\n".join(comments_str_list)
        comments_str = content

        created_date_list.append(create_date)
        external_id_list.append(external_id)
        object_id_list.append(object_id)
        issue_link_list.append(issue_link)
        created_by_list.append(created_by)
        created_at_list.append(created_at)
        error_type_list.append(error_type)
        error_code_list.append(error_code)
        page_start_list.append(page_start)
        page_end_list.append(page_end)
        remarks_list.append(comments_str)

    issue_df = pd.DataFrame()
    issue_df["Created Date"] = created_date_list
    issue_df["External ID"] = external_id_list
    issue_df["Object ID"] = object_id_list
    issue_df["Issue Link"] = issue_link_list
    issue_df["Created By"] = created_by_list
    issue_df["Created At"] = created_at_list
    issue_df["Error Type"] = error_type_list
    issue_df["Error Code"] = error_code_list
    issue_df["Page Start"] = page_start_list
    issue_df["Page End"] = page_end_list
    issue_df["Remarks"] = remarks_list

    created_at_date_list = []
    batch_name_list = []
    issue_link_list = []
    iteration_list = []
    external_id_list = []
    total_frames_list = []
    object_id_list = []
    object_in_start_frame_list = []
    object_in_end_frame_list = []
    start_frame_defect_list = []
    end_frame_defect_list = []
    geometry_shape_list = []
    class_list = []
    defect_type_list = []
    created_by_list = []
    remarks_list = []

    for image_index, asset in enumerate(json_export):
        if verbose & (image_index % verbose_frequency == 0):
            logger.info("Assets Processed: [" + str(num_assets) + "/" + str(image_index) + "]")
        # --------------------------------------------------------------------------------------------------------------
        asset_url = asset['asset']
        original_external_id = asset['externalId']
        external_id = re.sub("[^0-9]", "", original_external_id)
        if len(external_id) == 0:
            external_id = original_external_id

        batches = asset["batches"]
        batches_str = ", ".join(batches)

        total_frames = 1
        if 'dataset' in asset:
            total_frames = len(asset['dataset'])

        geometry_shape = '2D'
        tools = asset['task']["tools"]

        asset_title_list = []
        asset_page_list = []
        asset_object_id_list = []
        asset_track_id_list = []
        for tool in tools:
            title = tool['title']
            page = 0
            if 'page' in tool:
                page = int(tool["page"]) + 1

            object_id = tool['objectId']
            tool_classifications = tool['classifications']
            track_id = ""
            for classification in tool_classifications:
                if classification['title'] in ["TrackId", "TrackID", "trackId", "trackID", "trackid", "track_id"]:
                    track_id = classification['answer']

            asset_title_list.append(title)
            asset_page_list.append(page)
            asset_object_id_list.append(object_id)
            asset_track_id_list.append(track_id)

        asset_df = pd.DataFrame()
        asset_df["Title"] = asset_title_list
        asset_df["Page"] = asset_page_list
        asset_df["Object ID"] = asset_object_id_list
        asset_df["Track ID"] = asset_track_id_list

        unique_track_id_list = sorted(list(set(asset_df["Track ID"])))

        for track_id in unique_track_id_list:
            filtered_asset_df = asset_df[asset_df["Track ID"] == track_id]
            filtered_asset_df = filtered_asset_df.sort_values(by=["Page"], ascending=[True])
            filtered_asset_df = filtered_asset_df.reset_index(drop=True)

            filtered_asset_df["Page Increment"] = filtered_asset_df["Page"] + 1

            page_increment = list(filtered_asset_df["Page Increment"])
            page_increment = [page_increment[0]-1] + page_increment
            page_increment.pop()

            filtered_asset_df["Page Increment"] = page_increment

            filtered_asset_df["Page Difference"] = filtered_asset_df["Page"] - filtered_asset_df["Page Increment"]
            split_indices = filtered_asset_df.index[filtered_asset_df['Page Difference'] > 0].tolist()

            class_name = filtered_asset_df.iloc[0]['Title']

            issue_object_id_list = list(issue_df['Object ID'])
            if len(split_indices) == 0:
                object_in_start_frame = np.min(np.array(filtered_asset_df["Page"]))
                object_in_end_frame = np.max(np.array(filtered_asset_df["Page"]))

                issue_found = False
                for index in range(len(filtered_asset_df)):
                    current_object_id = filtered_asset_df.iloc[index]['Object ID']
                    if current_object_id in issue_object_id_list:
                        issue_found = True
                        break

                if issue_found:
                    for index in range(len(filtered_asset_df)):
                        current_object_id = filtered_asset_df.iloc[index]['Object ID']
                        if current_object_id in issue_object_id_list:
                            issue_indices = np.where(current_object_id == np.array(issue_object_id_list))[0]
                            for issue_index in issue_indices:
                                created_at_date_list.append(issue_df.iloc[issue_index]["Created Date"])
                                batch_name_list.append(batches_str)
                                issue_link_list.append(issue_df.iloc[issue_index]["Issue Link"])
                                iteration_list.append("")
                                external_id_list.append(external_id)
                                total_frames_list.append(total_frames)
                                object_id_list.append(track_id)
                                object_in_start_frame_list.append(object_in_start_frame)
                                object_in_end_frame_list.append(object_in_end_frame)
                                start_frame_defect_list.append(issue_df.iloc[issue_index]["Page Start"])
                                end_frame_defect_list.append(issue_df.iloc[issue_index]["Page End"])
                                geometry_shape_list.append(geometry_shape)
                                class_list.append(class_name)
                                defect_type_list.append(issue_df.iloc[issue_index]["Error Code"])
                                created_by_list.append(issue_df.iloc[issue_index]["Created By"])
                                remarks_list.append(issue_df.iloc[issue_index]["Remarks"])
                else:
                    created_at_date_list.append("")
                    batch_name_list.append(batches_str)
                    issue_link_list.append("")
                    iteration_list.append("")
                    external_id_list.append(external_id)
                    total_frames_list.append(total_frames)
                    object_id_list.append(track_id)
                    object_in_start_frame_list.append(object_in_start_frame)
                    object_in_end_frame_list.append(object_in_end_frame)
                    start_frame_defect_list.append("")
                    end_frame_defect_list.append("")
                    geometry_shape_list.append(geometry_shape)
                    class_list.append(class_name)
                    defect_type_list.append("NoError")
                    created_by_list.append("")
                    remarks_list.append("")
            else:
                split_indices = [0] + split_indices

                for ind, split_index in enumerate(split_indices):
                    start_index = split_index
                    if ind < len(split_indices)-1:
                        end_index = split_indices[ind+1]
                    else:
                        end_index = len(filtered_asset_df)

                    split_asset_df = filtered_asset_df.iloc[start_index:end_index, :]
                    split_asset_df = split_asset_df.reset_index(drop=True)

                    object_in_start_frame = np.min(np.array(split_asset_df["Page"]))
                    object_in_end_frame = np.max(np.array(split_asset_df["Page"]))

                    issue_found = False
                    for index in range(len(split_asset_df)):
                        current_object_id = split_asset_df.iloc[index]['Object ID']
                        if current_object_id in issue_object_id_list:
                            issue_found = True
                            break

                    if issue_found:
                        for index in range(len(split_asset_df)):
                            current_object_id = split_asset_df.iloc[index]['Object ID']
                            if current_object_id in issue_object_id_list:
                                issue_indices = np.where(current_object_id == np.array(issue_object_id_list))[0]
                                for issue_index in issue_indices:
                                    created_at_date_list.append(issue_df.iloc[issue_index]["Created Date"])
                                    batch_name_list.append(batches_str)
                                    issue_link_list.append(issue_df.iloc[issue_index]["Issue Link"])
                                    iteration_list.append("")
                                    external_id_list.append(external_id)
                                    total_frames_list.append(total_frames)
                                    object_id_list.append(track_id)
                                    object_in_start_frame_list.append(object_in_start_frame)
                                    object_in_end_frame_list.append(object_in_end_frame)
                                    start_frame_defect_list.append(issue_df.iloc[issue_index]["Page Start"])
                                    end_frame_defect_list.append(issue_df.iloc[issue_index]["Page End"])
                                    geometry_shape_list.append(geometry_shape)
                                    class_list.append(class_name)
                                    defect_type_list.append(issue_df.iloc[issue_index]["Error Code"])
                                    created_by_list.append(issue_df.iloc[issue_index]["Created By"])
                                    remarks_list.append(issue_df.iloc[issue_index]["Remarks"])
                    else:
                        created_at_date_list.append("")
                        batch_name_list.append(batches_str)
                        issue_link_list.append("")
                        iteration_list.append("")
                        external_id_list.append(external_id)
                        total_frames_list.append(total_frames)
                        object_id_list.append(track_id)
                        object_in_start_frame_list.append(object_in_start_frame)
                        object_in_end_frame_list.append(object_in_end_frame)
                        start_frame_defect_list.append("")
                        end_frame_defect_list.append("")
                        geometry_shape_list.append(geometry_shape)
                        class_list.append(class_name)
                        defect_type_list.append("NoError")
                        created_by_list.append("")
                        remarks_list.append("")
                pass

    data_df = pd.DataFrame()
    data_df['Date'] = created_at_date_list
    data_df['Work Package/Set'] = batch_name_list
    data_df['View Link'] = issue_link_list
    data_df['Iteration'] = iteration_list
    data_df['BatchID'] = external_id_list
    data_df['Total Frames'] = total_frames_list
    data_df['ObjectID'] = object_id_list
    data_df['Object in Start Frame'] = object_in_start_frame_list
    data_df['Object in End Frame'] = object_in_end_frame_list
    data_df['Start Frame Defect'] = start_frame_defect_list
    data_df['End Frame Defect'] = end_frame_defect_list
    data_df['Geometry Shape'] = geometry_shape_list
    data_df['Class'] = class_list
    data_df['Defect Type'] = defect_type_list
    data_df['Remarks'] = remarks_list
    data_df['Created By'] = created_by_list

    data_df = data_df.sort_values(by=["Work Package/Set", "BatchID", "Object in Start Frame", "Start Frame Defect"],
                                  ascending=[True, True, True, True])
    data_df = data_df.reset_index(drop=True)

    # ------------------------------------------------------------------------------------------------------------------

    # unique_work_package_list = list(set(list(data_df["Work Package/Set"])))
    # for unique_work_package in unique_work_package_list:
    #     work_package_data_df = data_df[data_df["Work Package/Set"] == unique_work_package]
    #     work_package_data_df = work_package_data_df.reset_index(drop=True)
    #
    #     unique_batch_id_list = list(set(list(data_df["BatchID"])))
    #     for unique_batch_id in unique_batch_id_list:
    #         batch_id_data_df = work_package_data_df[work_package_data_df["BatchID"] == unique_batch_id]
    #         batch_id_data_df = batch_id_data_df.reset_index(drop=True)
    #
    #         unique_object_id_list = list(set(list(batch_id_data_df["ObjectID"])))
    #
    #         for unique_object_id in unique_object_id_list:
    #             object_id_data_df = batch_id_data_df[batch_id_data_df["ObjectID"] == unique_object_id]
    #             object_id_data_df = object_id_data_df.reset_index(drop=True)
    #
    #             object_in_start_frame = object_id_data_df['Object in Start Frame'].iloc[0]
    #             object_in_end_frame = object_id_data_df['Object in End Frame'].iloc[0]
    #
    #             interval_indices = np.arange(object_in_start_frame, object_in_end_frame+1)
    #             interval_flags = np.zeros_like(interval_indices)
    #
    #             for obj_index in range(len(object_id_data_df)):
    #                 defect_type = object_id_data_df['Defect Type'].iloc[obj_index]
    #                 if defect_type == "NoError":
    #                     continue
    #
    #                 start_frame_defect = object_id_data_df['Start Frame Defect'].iloc[obj_index]
    #                 end_frame_defect = object_id_data_df['End Frame Defect'].iloc[obj_index]
    #
    #                 index_1 = np.where(interval_indices == start_frame_defect)[0][0]
    #                 index_2 = np.where(interval_indices == end_frame_defect)[0][0]
    #
    #                 interval_flags[index_1:index_2] = True

    # ------------------------------------------------------------------------------------------------------------------
    # Separate intervals
    processed_data_df = pd.DataFrame()
    for index in range(len(data_df)):
        error_type = data_df['Defect Type'].iloc[index]
        if error_type == "NoError":
            processed_data_df = processed_data_df.append(data_df.iloc[index, :], ignore_index=True)
            continue

        object_in_start_frame = data_df['Object in Start Frame'].iloc[index]
        object_in_end_frame = data_df['Object in End Frame'].iloc[index]
        start_frame_defect = data_df['Start Frame Defect'].iloc[index]
        end_frame_defect = data_df['End Frame Defect'].iloc[index]

        if (object_in_start_frame == start_frame_defect) & (object_in_end_frame == end_frame_defect):
            processed_data_df = processed_data_df.append(data_df.iloc[index, :], ignore_index=True)
            continue

        batch_id = data_df['BatchID'].iloc[index]
        object_id = data_df['ObjectID'].iloc[index]

        current_df = data_df[(data_df['BatchID'] == batch_id) & (data_df['ObjectID'] == object_id) & (data_df['Defect Type'] != "NoError")]
        current_df = current_df.reset_index(drop=True)

        if len(current_df) > 1:
            # Needs development
            print('Warning! len(current_df) = ' + str(len(current_df)))

        # Add the beginning of the interval
        if start_frame_defect > object_in_start_frame:
            start_df = pd.DataFrame()
            start_df = data_df.iloc[index, :].copy()
            start_df['Date'] = ""
            start_df['View Link'] = ""
            start_df['Object in Start Frame'] = object_in_start_frame
            start_df['Object in End Frame'] = start_frame_defect-1
            start_df['Defect Type'] = "NoError"
            start_df['Remarks'] = ""
            start_df['Created By'] = ""

            processed_data_df = processed_data_df.append(start_df, ignore_index=True)

        # Add the issue
        current_issue_df = pd.DataFrame()
        current_issue_df = data_df.iloc[index, :].copy()
        current_issue_df['Object in Start Frame'] = start_frame_defect
        current_issue_df['Object in End Frame'] = end_frame_defect

        processed_data_df = processed_data_df.append(current_issue_df, ignore_index=True)

        # Add the ending of the interval
        if end_frame_defect < object_in_end_frame:
            end_df = pd.DataFrame()
            end_df = data_df.iloc[index, :].copy()
            end_df['Date'] = ""
            end_df['View Link'] = ""
            end_df['Object in Start Frame'] = end_frame_defect+1
            end_df['Object in End Frame'] = object_in_end_frame
            end_df['Defect Type'] = "NoError"
            end_df['Remarks'] = ""
            end_df['Created By'] = ""

            processed_data_df = processed_data_df.append(end_df, ignore_index=True)

    processed_data_df = processed_data_df.drop(columns=["Start Frame Defect", "End Frame Defect"])
    # ------------------------------------------------------------------------------------------------------------------
    # Add missing type of issues
    missing_issue_df = issue_df[issue_df['Object ID'] == ""]
    missing_issue_df = missing_issue_df.reset_index(drop=True)

    for index in range(len(missing_issue_df)):
        external_id = missing_issue_df.iloc[index]["External ID"]
        if external_id not in list(processed_data_df['BatchID']):
            continue

        current_processed_data_df = processed_data_df[processed_data_df['BatchID'] == external_id]
        current_processed_data_df = current_processed_data_df.reset_index(drop=True)

        remarks = missing_issue_df.iloc[index]["Remarks"]
        defect_type = missing_issue_df.iloc[index]["Error Code"]

        # Find Object ID and Class
        class_str = ""
        object_id = ""
        defect_type_lower = defect_type.lower()

        auto_id = False
        if remarks is None:
            auto_id = True
        else:
            if len(remarks) == 0:
                auto_id = True

        if defect_type_lower == "missingtrafficlightbulb":
            class_str = "TrafficLightBulb"
            if auto_id:
                class_based_data_df = current_processed_data_df[current_processed_data_df["Class"] == "TrafficLightBulb"]
                class_based_data_df = class_based_data_df.reset_index(drop=True)
                max_id = 0
                if len(class_based_data_df) > 0:
                    max_id = class_based_data_df["ObjectID"].str[3:].astype(int).max()
                object_id = "TLB" + str(max_id+1)
            else:
                object_id = str(remarks)

        elif defect_type_lower == "missingtrafficlighthousing":
            class_str = "TrafficLightHousing"
            if auto_id:
                class_based_data_df = current_processed_data_df[current_processed_data_df["Class"] == "TrafficLightHousing"]
                class_based_data_df = class_based_data_df.reset_index(drop=True)
                max_id = 0
                if len(class_based_data_df) > 0:
                    max_id = class_based_data_df["ObjectID"].str[2:].astype(int).max()
                object_id = "TL" + str(max_id + 1)
            else:
                object_id = str(remarks)

        current_issue_df = pd.DataFrame()
        current_issue_df['Date'] = [missing_issue_df.iloc[index]["Created Date"]]
        current_issue_df['Work Package/Set'] = [current_processed_data_df["Work Package/Set"].iloc[0]]
        current_issue_df['View Link'] = [missing_issue_df.iloc[index]["Issue Link"]]
        current_issue_df['Iteration'] = [""]
        current_issue_df['BatchID'] = [external_id]
        current_issue_df['Total Frames'] = [current_processed_data_df["Total Frames"].iloc[0]]
        current_issue_df['ObjectID'] = [object_id]
        current_issue_df['Object in Start Frame'] = [missing_issue_df.iloc[index]["Page Start"]]
        current_issue_df['Object in End Frame'] = [missing_issue_df.iloc[index]["Page End"]]
        current_issue_df['Geometry Shape'] = ['2D']
        current_issue_df['Class'] = [class_str]
        current_issue_df['Defect Type'] = [defect_type]
        current_issue_df['Remarks'] = [remarks]
        current_issue_df['Created By'] = [missing_issue_df.iloc[index]["Created By"]]

        processed_data_df = processed_data_df.append(current_issue_df, ignore_index=True)

    logger.info("Issues Processed: [" + str(num_issues) + "/" + str(num_issues) + "]")

    processed_data_df = processed_data_df.sort_values(by=["Work Package/Set", "BatchID", "Object in Start Frame", "Object in End Frame"],
                                                      ascending=[True, True, True, True])
    processed_data_df = processed_data_df.reset_index(drop=True)


    time_str = str(datetime.utcnow())
    time_str = time_str[:-3].replace(' ', 'T') + 'Z'
    time_str = time_str.replace(':', '-')

    output_filename = project_id + '-export-' + time_str + '_[Issue].csv'
    processed_data_df.to_csv(output_filename, index=False)

    zip_filename = project_id + '-export-' + time_str + '.zip'
    mem_zip = BytesIO()
    with zipfile.ZipFile(mem_zip, mode="w") as zf:
        zf.write(output_filename)
    os.remove(output_filename)

    print_session_end(logger)

    export_response_obj = ExportResponse(file=mem_zip, file_name=zip_filename, storage_id="", bucket="")
    return export_response_obj


def issue_export_bosch_test(data):
    # Extract input parameters
    project_id = data.get('projectId')
    json_export = data.get('jsonExport')
    num_assets = data.get('numTasks')
    api_key = data.get("apiKey")
    logger = data.get('logger')
    config_str = data.get('configJSON')
    try:
        config = json.loads(config_str)
    except Exception as error:
        print(error)
        message = 'The config JSON format is wrong!'
        print_info_message(logger, message)
        message = 'Default parameters are used.'
        print_info_message(logger, message)
        config = {}
    # -------------------------------------------------
    print_session_start(logger)
    print_session_info(data)

    print_info_message(logger, "The number of exported assets: " + str(num_assets))
    if num_assets == 0:
        raise Exception("Empty export exception!")

    add_assignee_info, extract_metrics, verbose, verbose_frequency = check_config(config)

    sdk = SDK(api_key=api_key, host=HOST)

    original_issue_list = sdk.exportV3(project_id, export_type="issue")

    issue_list = []
    for issue in original_issue_list:
        if issue["status"] == "Open":
            if ("errorType" in issue) & ("errorCode" in issue):
                if (issue["errorType"] == "Unqualified") & (issue["errorCode"].lower() == "feedback"):
                    continue
            issue_list.append(issue)
    num_issues = len(issue_list)

    created_date_list = []
    external_id_list = []
    object_id_list = []
    issue_link_list = []
    created_by_list = []
    created_at_list = []
    error_type_list = []
    error_code_list = []
    page_start_list = []
    page_end_list = []
    remarks_list = []
    for issue in issue_list:
        original_external_id = issue["asset"]["externalId"]
        external_id = re.sub("[^0-9]", "", original_external_id)
        if len(external_id) == 0:
            external_id = original_external_id

        object_id = ""
        if 'objectId' in issue:
            object_id = issue['objectId']
        issue_id = issue["_id"]
        label_task = issue["labelTask"]
        issue_link = "https://imerit.ango.ai/issue/" + label_task + "?issue=" + issue_id

        created_by = issue["createdBy"]
        created_at = issue["createdAt"]

        error_type = ""
        if 'errorType' in issue:
            error_type = issue['errorType']

        error_code = ""
        if 'errorCode' in issue:
            error_code = issue['errorCode']

        page_start = 0
        if 'page' in issue:
            page_start = int(issue["page"]) + 1

        page_end = 0
        if 'pageEnd' in issue:
            page_end = int(issue["pageEnd"]) + 1

        content = ""
        if 'content' in issue:
            content = issue['content']

        comments = ""
        if 'comments' in issue:
            comments = issue['comments']

        # Calculate Time Fields
        time_split = created_at.split('T')
        created_date = time_split[0]
        created_time = time_split[1][0:8]

        year, month, day = created_date.split('-')
        hour, minute, second = created_time.split(':')

        created_datetime = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
        create_date = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2)

        # comments_str_list = []
        # comments_str_list.append(created_by + ": " + content)
        # for comment in comments:
        #     comments_str_list.append(comment["createdBy"] + ": " + comment["content"])
        # comments_str = "\n".join(comments_str_list)
        comments_str = content

        created_date_list.append(create_date)
        external_id_list.append(external_id)
        object_id_list.append(object_id)
        issue_link_list.append(issue_link)
        created_by_list.append(created_by)
        created_at_list.append(created_at)
        error_type_list.append(error_type)
        error_code_list.append(error_code)
        page_start_list.append(page_start)
        page_end_list.append(page_end)
        remarks_list.append(comments_str)

    issue_df = pd.DataFrame()
    issue_df["Created Date"] = created_date_list
    issue_df["External ID"] = external_id_list
    issue_df["Object ID"] = object_id_list
    issue_df["Issue Link"] = issue_link_list
    issue_df["Created By"] = created_by_list
    issue_df["Created At"] = created_at_list
    issue_df["Error Type"] = error_type_list
    issue_df["Error Code"] = error_code_list
    issue_df["Page Start"] = page_start_list
    issue_df["Page End"] = page_end_list
    issue_df["Remarks"] = remarks_list

    created_at_date_list = []
    batch_name_list = []
    issue_link_list = []
    iteration_list = []
    external_id_list = []
    total_frames_list = []
    object_id_list = []
    object_in_start_frame_list = []
    object_in_end_frame_list = []
    start_frame_defect_list = []
    end_frame_defect_list = []
    geometry_shape_list = []
    class_list = []
    defect_type_list = []
    created_by_list = []
    remarks_list = []

    for image_index, asset in enumerate(json_export):
        if verbose & (image_index % verbose_frequency == 0):
            logger.info("Assets Processed: [" + str(num_assets) + "/" + str(image_index) + "]")
        # --------------------------------------------------------------------------------------------------------------
        asset_url = asset['asset']
        original_external_id = asset['externalId']
        external_id = re.sub("[^0-9]", "", original_external_id)
        if len(external_id) == 0:
            external_id = original_external_id

        batches = asset["batches"]
        batches_str = ", ".join(batches)

        total_frames = 1
        if 'dataset' in asset:
            total_frames = len(asset['dataset'])

        geometry_shape = '2D'
        tools = asset['task']["tools"]

        asset_title_list = []
        asset_page_list = []
        asset_object_id_list = []
        asset_track_id_list = []
        for tool in tools:
            title = tool['title']
            page = 0
            if 'page' in tool:
                page = int(tool["page"]) + 1

            object_id = tool['objectId']
            tool_classifications = tool['classifications']
            track_id = ""
            for classification in tool_classifications:
                if classification['title'] in ["TrackId", "TrackID", "trackId", "trackID", "trackid", "track_id"]:
                    track_id = classification['answer']

            asset_title_list.append(title)
            asset_page_list.append(page)
            asset_object_id_list.append(object_id)
            asset_track_id_list.append(track_id)

        asset_df = pd.DataFrame()
        asset_df["Title"] = asset_title_list
        asset_df["Page"] = asset_page_list
        asset_df["Object ID"] = asset_object_id_list
        asset_df["Track ID"] = asset_track_id_list

        unique_track_id_list = sorted(list(set(asset_df["Track ID"])))

        for track_id in unique_track_id_list:
            filtered_asset_df = asset_df[asset_df["Track ID"] == track_id]
            filtered_asset_df = filtered_asset_df.sort_values(by=["Page"], ascending=[True])
            filtered_asset_df = filtered_asset_df.reset_index(drop=True)

            filtered_asset_df["Page Increment"] = filtered_asset_df["Page"] + 1

            page_increment = list(filtered_asset_df["Page Increment"])
            page_increment = [page_increment[0]-1] + page_increment
            page_increment.pop()

            filtered_asset_df["Page Increment"] = page_increment

            filtered_asset_df["Page Difference"] = filtered_asset_df["Page"] - filtered_asset_df["Page Increment"]
            split_indices = filtered_asset_df.index[filtered_asset_df['Page Difference'] > 0].tolist()

            class_name = filtered_asset_df.iloc[0]['Title']

            issue_object_id_list = list(issue_df['Object ID'])
            if len(split_indices) == 0:
                object_in_start_frame = np.min(np.array(filtered_asset_df["Page"]))
                object_in_end_frame = np.max(np.array(filtered_asset_df["Page"]))

                issue_found = False
                for index in range(len(filtered_asset_df)):
                    current_object_id = filtered_asset_df.iloc[index]['Object ID']
                    if current_object_id in issue_object_id_list:
                        issue_found = True
                        break

                if issue_found:
                    for index in range(len(filtered_asset_df)):
                        current_object_id = filtered_asset_df.iloc[index]['Object ID']
                        if current_object_id in issue_object_id_list:
                            issue_indices = np.where(current_object_id == np.array(issue_object_id_list))[0]
                            for issue_index in issue_indices:
                                created_at_date_list.append(issue_df.iloc[issue_index]["Created Date"])
                                batch_name_list.append(batches_str)
                                issue_link_list.append(issue_df.iloc[issue_index]["Issue Link"])
                                iteration_list.append("")
                                external_id_list.append(external_id)
                                total_frames_list.append(total_frames)
                                object_id_list.append(track_id)
                                object_in_start_frame_list.append(object_in_start_frame)
                                object_in_end_frame_list.append(object_in_end_frame)
                                start_frame_defect_list.append(issue_df.iloc[issue_index]["Page Start"])
                                end_frame_defect_list.append(issue_df.iloc[issue_index]["Page End"])
                                geometry_shape_list.append(geometry_shape)
                                class_list.append(class_name)
                                defect_type_list.append(issue_df.iloc[issue_index]["Error Code"])
                                created_by_list.append(issue_df.iloc[issue_index]["Created By"])
                                remarks_list.append(issue_df.iloc[issue_index]["Remarks"])
                else:
                    created_at_date_list.append("")
                    batch_name_list.append(batches_str)
                    issue_link_list.append("")
                    iteration_list.append("")
                    external_id_list.append(external_id)
                    total_frames_list.append(total_frames)
                    object_id_list.append(track_id)
                    object_in_start_frame_list.append(object_in_start_frame)
                    object_in_end_frame_list.append(object_in_end_frame)
                    start_frame_defect_list.append("")
                    end_frame_defect_list.append("")
                    geometry_shape_list.append(geometry_shape)
                    class_list.append(class_name)
                    defect_type_list.append("NoError")
                    created_by_list.append("")
                    remarks_list.append("")
            else:
                split_indices = [0] + split_indices

                for ind, split_index in enumerate(split_indices):
                    start_index = split_index
                    if ind < len(split_indices)-1:
                        end_index = split_indices[ind+1]
                    else:
                        end_index = len(filtered_asset_df)

                    split_asset_df = filtered_asset_df.iloc[start_index:end_index, :]
                    split_asset_df = split_asset_df.reset_index(drop=True)

                    object_in_start_frame = np.min(np.array(split_asset_df["Page"]))
                    object_in_end_frame = np.max(np.array(split_asset_df["Page"]))

                    issue_found = False
                    for index in range(len(split_asset_df)):
                        current_object_id = split_asset_df.iloc[index]['Object ID']
                        if current_object_id in issue_object_id_list:
                            issue_found = True
                            break

                    if issue_found:
                        for index in range(len(split_asset_df)):
                            current_object_id = split_asset_df.iloc[index]['Object ID']
                            if current_object_id in issue_object_id_list:
                                issue_indices = np.where(current_object_id == np.array(issue_object_id_list))[0]
                                for issue_index in issue_indices:
                                    created_at_date_list.append(issue_df.iloc[issue_index]["Created Date"])
                                    batch_name_list.append(batches_str)
                                    issue_link_list.append(issue_df.iloc[issue_index]["Issue Link"])
                                    iteration_list.append("")
                                    external_id_list.append(external_id)
                                    total_frames_list.append(total_frames)
                                    object_id_list.append(track_id)
                                    object_in_start_frame_list.append(object_in_start_frame)
                                    object_in_end_frame_list.append(object_in_end_frame)
                                    start_frame_defect_list.append(issue_df.iloc[issue_index]["Page Start"])
                                    end_frame_defect_list.append(issue_df.iloc[issue_index]["Page End"])
                                    geometry_shape_list.append(geometry_shape)
                                    class_list.append(class_name)
                                    defect_type_list.append(issue_df.iloc[issue_index]["Error Code"])
                                    created_by_list.append(issue_df.iloc[issue_index]["Created By"])
                                    remarks_list.append(issue_df.iloc[issue_index]["Remarks"])
                    else:
                        created_at_date_list.append("")
                        batch_name_list.append(batches_str)
                        issue_link_list.append("")
                        iteration_list.append("")
                        external_id_list.append(external_id)
                        total_frames_list.append(total_frames)
                        object_id_list.append(track_id)
                        object_in_start_frame_list.append(object_in_start_frame)
                        object_in_end_frame_list.append(object_in_end_frame)
                        start_frame_defect_list.append("")
                        end_frame_defect_list.append("")
                        geometry_shape_list.append(geometry_shape)
                        class_list.append(class_name)
                        defect_type_list.append("NoError")
                        created_by_list.append("")
                        remarks_list.append("")
                pass

    data_df = pd.DataFrame()
    data_df['Date'] = created_at_date_list
    data_df['Work Package/Set'] = batch_name_list
    data_df['View Link'] = issue_link_list
    data_df['Iteration'] = iteration_list
    data_df['BatchID'] = external_id_list
    data_df['Total Frames'] = total_frames_list
    data_df['ObjectID'] = object_id_list
    data_df['Object in Start Frame'] = object_in_start_frame_list
    data_df['Object in End Frame'] = object_in_end_frame_list
    data_df['Start Frame Defect'] = start_frame_defect_list
    data_df['End Frame Defect'] = end_frame_defect_list
    data_df['Geometry Shape'] = geometry_shape_list
    data_df['Class'] = class_list
    data_df['Defect Type'] = defect_type_list
    data_df['Remarks'] = remarks_list
    data_df['Created By'] = created_by_list

    data_df = data_df.sort_values(by=["Work Package/Set", "BatchID", "Object in Start Frame", "Start Frame Defect"],
                                  ascending=[True, True, True, True])
    data_df = data_df.reset_index(drop=True)

    # ------------------------------------------------------------------------------------------------------------------

    # unique_work_package_list = list(set(list(data_df["Work Package/Set"])))
    # for unique_work_package in unique_work_package_list:
    #     work_package_data_df = data_df[data_df["Work Package/Set"] == unique_work_package]
    #     work_package_data_df = work_package_data_df.reset_index(drop=True)
    #
    #     unique_batch_id_list = list(set(list(data_df["BatchID"])))
    #     for unique_batch_id in unique_batch_id_list:
    #         batch_id_data_df = work_package_data_df[work_package_data_df["BatchID"] == unique_batch_id]
    #         batch_id_data_df = batch_id_data_df.reset_index(drop=True)
    #
    #         unique_object_id_list = list(set(list(batch_id_data_df["ObjectID"])))
    #
    #         for unique_object_id in unique_object_id_list:
    #             object_id_data_df = batch_id_data_df[batch_id_data_df["ObjectID"] == unique_object_id]
    #             object_id_data_df = object_id_data_df.reset_index(drop=True)
    #
    #             object_in_start_frame = object_id_data_df['Object in Start Frame'].iloc[0]
    #             object_in_end_frame = object_id_data_df['Object in End Frame'].iloc[0]
    #
    #             interval_indices = np.arange(object_in_start_frame, object_in_end_frame+1)
    #             interval_flags = np.zeros_like(interval_indices)
    #
    #             for obj_index in range(len(object_id_data_df)):
    #                 defect_type = object_id_data_df['Defect Type'].iloc[obj_index]
    #                 if defect_type == "NoError":
    #                     continue
    #
    #                 start_frame_defect = object_id_data_df['Start Frame Defect'].iloc[obj_index]
    #                 end_frame_defect = object_id_data_df['End Frame Defect'].iloc[obj_index]
    #
    #                 index_1 = np.where(interval_indices == start_frame_defect)[0][0]
    #                 index_2 = np.where(interval_indices == end_frame_defect)[0][0]
    #
    #                 interval_flags[index_1:index_2] = True

    # ------------------------------------------------------------------------------------------------------------------
    # Separate intervals
    processed_data_df = pd.DataFrame()
    for index in range(len(data_df)):
        error_type = data_df['Defect Type'].iloc[index]
        if error_type == "NoError":
            processed_data_df = processed_data_df.append(data_df.iloc[index, :], ignore_index=True)
            continue

        object_in_start_frame = data_df['Object in Start Frame'].iloc[index]
        object_in_end_frame = data_df['Object in End Frame'].iloc[index]
        start_frame_defect = data_df['Start Frame Defect'].iloc[index]
        end_frame_defect = data_df['End Frame Defect'].iloc[index]

        if (object_in_start_frame == start_frame_defect) & (object_in_end_frame == end_frame_defect):
            processed_data_df = processed_data_df.append(data_df.iloc[index, :], ignore_index=True)
            continue

        batch_id = data_df['BatchID'].iloc[index]
        object_id = data_df['ObjectID'].iloc[index]

        current_df = data_df[(data_df['BatchID'] == batch_id) & (data_df['ObjectID'] == object_id) & (data_df['Defect Type'] != "NoError")]
        current_df = current_df.reset_index(drop=True)

        if len(current_df) > 1:
            # Needs development
            print('Warning! len(current_df) = ' + str(len(current_df)))

        # Add the beginning of the interval
        if start_frame_defect > object_in_start_frame:
            start_df = pd.DataFrame()
            start_df = data_df.iloc[index, :].copy()
            start_df['Date'] = ""
            start_df['View Link'] = ""
            start_df['Object in Start Frame'] = object_in_start_frame
            start_df['Object in End Frame'] = start_frame_defect-1
            start_df['Defect Type'] = "NoError"
            start_df['Remarks'] = ""
            start_df['Created By'] = ""

            processed_data_df = processed_data_df.append(start_df, ignore_index=True)

        # Add the issue
        current_issue_df = pd.DataFrame()
        current_issue_df = data_df.iloc[index, :].copy()
        current_issue_df['Object in Start Frame'] = start_frame_defect
        current_issue_df['Object in End Frame'] = end_frame_defect

        processed_data_df = processed_data_df.append(current_issue_df, ignore_index=True)

        # Add the ending of the interval
        if end_frame_defect < object_in_end_frame:
            end_df = pd.DataFrame()
            end_df = data_df.iloc[index, :].copy()
            end_df['Date'] = ""
            end_df['View Link'] = ""
            end_df['Object in Start Frame'] = end_frame_defect+1
            end_df['Object in End Frame'] = object_in_end_frame
            end_df['Defect Type'] = "NoError"
            end_df['Remarks'] = ""
            end_df['Created By'] = ""

            processed_data_df = processed_data_df.append(end_df, ignore_index=True)

    processed_data_df = processed_data_df.drop(columns=["Start Frame Defect", "End Frame Defect"])
    # ------------------------------------------------------------------------------------------------------------------
    # Add missing type of issues
    missing_issue_df = issue_df[issue_df['Object ID'] == ""]
    missing_issue_df = missing_issue_df.reset_index(drop=True)

    for index in range(len(missing_issue_df)):
        external_id = missing_issue_df.iloc[index]["External ID"]
        if external_id not in list(processed_data_df['BatchID']):
            continue

        current_processed_data_df = processed_data_df[processed_data_df['BatchID'] == external_id]
        current_processed_data_df = current_processed_data_df.reset_index(drop=True)

        remarks = missing_issue_df.iloc[index]["Remarks"]
        defect_type = missing_issue_df.iloc[index]["Error Code"]

        # Find Object ID and Class
        class_str = ""
        object_id = ""
        defect_type_lower = defect_type.lower()

        auto_id = False
        if remarks is None:
            auto_id = True
        else:
            if len(remarks) == 0:
                auto_id = True

        if defect_type_lower == "missingtrafficlightbulb":
            class_str = "TrafficLightBulb"
            if auto_id:
                class_based_data_df = current_processed_data_df[current_processed_data_df["Class"] == "TrafficLightBulb"]
                class_based_data_df = class_based_data_df.reset_index(drop=True)
                max_id = 0
                if len(class_based_data_df) > 0:
                    max_id = class_based_data_df["ObjectID"].str[3:].astype(int).max()
                object_id = "TLB" + str(max_id+1)
            else:
                object_id = str(remarks)

        elif defect_type_lower == "missingtrafficlighthousing":
            class_str = "TrafficLightHousing"
            if auto_id:
                class_based_data_df = current_processed_data_df[current_processed_data_df["Class"] == "TrafficLightHousing"]
                class_based_data_df = class_based_data_df.reset_index(drop=True)
                max_id = 0
                if len(class_based_data_df) > 0:
                    max_id = class_based_data_df["ObjectID"].str[2:].astype(int).max()
                object_id = "TL" + str(max_id + 1)
            else:
                object_id = str(remarks)

        current_issue_df = pd.DataFrame()
        current_issue_df['Date'] = [missing_issue_df.iloc[index]["Created Date"]]
        current_issue_df['Work Package/Set'] = [current_processed_data_df["Work Package/Set"].iloc[0]]
        current_issue_df['View Link'] = [missing_issue_df.iloc[index]["Issue Link"]]
        current_issue_df['Iteration'] = [""]
        current_issue_df['BatchID'] = [external_id]
        current_issue_df['Total Frames'] = [current_processed_data_df["Total Frames"].iloc[0]]
        current_issue_df['ObjectID'] = [object_id]
        current_issue_df['Object in Start Frame'] = [missing_issue_df.iloc[index]["Page Start"]]
        current_issue_df['Object in End Frame'] = [missing_issue_df.iloc[index]["Page End"]]
        current_issue_df['Geometry Shape'] = ['2D']
        current_issue_df['Class'] = [class_str]
        current_issue_df['Defect Type'] = [defect_type]
        current_issue_df['Remarks'] = [remarks]
        current_issue_df['Created By'] = [missing_issue_df.iloc[index]["Created By"]]

        processed_data_df = processed_data_df.append(current_issue_df, ignore_index=True)

    logger.info("Issues Processed: [" + str(num_issues) + "/" + str(num_issues) + "]")

    processed_data_df = processed_data_df.sort_values(by=["Work Package/Set", "BatchID", "Object in Start Frame", "Object in End Frame"],
                                                      ascending=[True, True, True, True])
    processed_data_df = processed_data_df.reset_index(drop=True)


    time_str = str(datetime.utcnow())
    time_str = time_str[:-3].replace(' ', 'T') + 'Z'
    time_str = time_str.replace(':', '-')

    output_filename = project_id + '-export-' + time_str + '_[Issue].csv'
    processed_data_df.to_csv(output_filename, index=False)

    zip_filename = project_id + '-export-' + time_str + '.zip'
    mem_zip = BytesIO()
    with zipfile.ZipFile(mem_zip, mode="w") as zf:
        zf.write(output_filename)
    os.remove(output_filename)

    print_session_end(logger)

    export_response_obj = ExportResponse(file=mem_zip, file_name=zip_filename, storage_id="", bucket="")
    return export_response_obj


if __name__ == "__main__":
    plugin = ExportPlugin(id=PLUGIN_ID,
                          secret=PLUGIN_SECRET,
                          callback=callback_function,
                          host=HOST,
                          version='v3')

    run(plugin, host=HOST)
