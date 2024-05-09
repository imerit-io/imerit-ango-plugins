import os
import re
import json
import math
import zipfile
import traceback
import pandas as pd
from io import BytesIO
from datetime import datetime
from imerit_ango.sdk import SDK
from dateutil.relativedelta import relativedelta
from imerit_ango.plugins import ExportPlugin, ExportResponse, run

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
    date_regex = re.compile(r"\d\d\d\d-\d\d-\d\d")

    start_date = None
    if "start_date" in config:
        if isinstance(config['start_date'], str):
            start_date = config['start_date']
            if date_regex.match(start_date) is None:
                start_date = None

    end_date = None
    if "end_date" in config:
        if isinstance(config['end_date'], str):
            end_date = config['end_date']
            if date_regex.match(end_date) is None:
                end_date = None

    duration_unit = "sec"
    if "duration_unit" in config:
        if isinstance(config['duration_unit'], str):
            duration_unit = config['duration_unit']

    gold_stage = None
    if 'gold_stage' in config:
        if isinstance(config['gold_stage'], str):
            gold_stage = config['gold_stage']

    evaluation_stage = None
    if 'evaluation_stage' in config:
        if isinstance(config['evaluation_stage'], str):
            evaluation_stage = config['evaluation_stage']

    ignored_schema_ids = []
    if 'ignored_schema_ids' in config:
        if isinstance(config['ignored_schema_ids'], list):
            ignored_schema_ids = config['ignored_schema_ids']

    verbose = True
    if 'verbose' in config:
        if isinstance(config['verbose'], bool):
            verbose = config['verbose']

    verbose_frequency = 10
    if 'verbose_frequency' in config:
        if isinstance(config['verbose_frequency'], int):
            if config['verbose_frequency'] > 0:
                verbose_frequency = config['verbose_frequency']

    return start_date, end_date, duration_unit, gold_stage, evaluation_stage, ignored_schema_ids, verbose, verbose_frequency


def scrape_tools(tool_list, tool_tree=None, classification_dict_list=None):
    if tool_tree is None:
        tool_tree = []
    if classification_dict_list is None:
        classification_dict_list = []

    for classification_tool in tool_list:
        # print(classification_tool['title'], '-', classification_tool['schemaId'], '->', tool_tree)
        classification_dict_list.append({'title': classification_tool['title'],
                                         'schemaId': classification_tool['schemaId'],
                                         'tool_tree': tool_tree.copy(),
                                         'tool_type': classification_tool['tool'],
                                         'level': len(tool_tree.copy())})

        if 'classifications' in classification_tool:
            tool_tree_org = tool_tree.copy()
            tool_tree.append(classification_tool['title'])
            scrape_tools(classification_tool['classifications'], tool_tree, classification_dict_list)
            tool_tree = tool_tree_org


def scrape_answers(tool_list, tool_tree=None, classification_answer_dict_list=None):
    if tool_tree is None:
        tool_tree = []
    if classification_answer_dict_list is None:
        classification_answer_dict_list = []

    for classification_tool in tool_list:
        answer = ''
        if 'answer' in classification_tool:
            answer = classification_tool['answer']
        # print(classification_tool['title'], '-', classification_tool['schemaId'], '-', answer, '->', tool_tree)
        classification_answer_dict_list.append({'title': classification_tool['title'],
                                                'schemaId': classification_tool['schemaId'],
                                                'answer': answer,
                                                'tool_tree': tool_tree.copy(),
                                                'tool_type': classification_tool['tool'],
                                                'level': len(tool_tree.copy())})

        if 'classifications' in classification_tool:
            tool_tree_org = tool_tree.copy()
            tool_tree.append(classification_tool['title'])
            scrape_answers(classification_tool['classifications'], tool_tree, classification_answer_dict_list)
            tool_tree = tool_tree_org


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


def check_date(stage_history, start_date, end_date):
    if (start_date is None) & (end_date is None):
        return True
    if start_date is None:
        start_date = "2020-12-31"
    if end_date is None:
        end_date = "2100-01-01"

    date_format = '%Y-%m-%d'
    start_datetime = datetime.strptime(start_date, date_format)
    end_datetime = datetime.strptime(end_date, date_format)

    for current_stage in stage_history:
        if 'completedAt' in current_stage:
            completed_at = current_stage['completedAt']
            time_split = completed_at.split('T')
            completed_date = time_split[0]
            year, month, day = completed_date.split('-')
            label_date = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2)
            label_datetime = datetime.strptime(label_date, date_format)
            # Compare date
            if start_datetime <= label_datetime <= end_datetime:
                return True
    return False


def callback_function(**data):
    logger = data.get('logger')

    try:
        return stage_export(data)
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


def stage_export(data):
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

    start_date, end_date, duration_unit, gold_stage, evaluation_stage, ignored_schema_ids, verbose, verbose_frequency = check_config(config)

    sdk = SDK(api_key=api_key, host=HOST)

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
        if stage['name'] in ['Complete']:
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

    # Prepare stage info dictionary
    dict_stage_info = {}
    for index in range(len(stage_df)):
        stage_name = stage_df["stage_name"].iloc[index]
        stage_type = stage_df["stage_type"].iloc[index]
        if stage_type == 'Label':
            dict_stage_info[stage_name] = {}
            dict_stage_info[stage_name]['labeler_name'] = []
            dict_stage_info[stage_name]['labeling_date'] = []
            dict_stage_info[stage_name]['labeling_start_time'] = []
            dict_stage_info[stage_name]['labeling_end_time'] = []
            dict_stage_info[stage_name]['labeling_duration'] = []
        if stage_type == 'Review':
            dict_stage_info[stage_name] = {}
            dict_stage_info[stage_name]['reviewer_name'] = []
            dict_stage_info[stage_name]['review_date'] = []
            dict_stage_info[stage_name]['review_start_time'] = []
            dict_stage_info[stage_name]['review_end_time'] = []
            dict_stage_info[stage_name]['review_duration'] = []
            dict_stage_info[stage_name]['review_task_status'] = []

    # Prepare count dictionary
    dict_stage_count = {}
    for index in range(len(stage_df)):
        stage_name = stage_df["stage_name"].iloc[index]
        stage_type = stage_df["stage_type"].iloc[index]
        if (stage_type == "Label") | (stage_type == "Review") | (stage_type == "Start"):
            dict_stage_count[stage_name] = {}
            dict_stage_count[stage_name]["number_of_classifications"] = []
            dict_stage_count[stage_name]["number_of_tools"] = []
            dict_stage_count[stage_name]["number_of_tool_classifications"] = []
            dict_stage_count[stage_name]["number_of_relations"] = []

    # Get all nested classification tools recursively
    classification_dict_list = []
    classification_tool_list = project_ontology['classifications']
    scrape_tools(classification_tool_list, tool_tree=[], classification_dict_list=classification_dict_list)

    dict_list_answer_lists = {}
    for index in range(len(stage_df)):
        stage_name = stage_df["stage_name"].iloc[index]
        stage_type = stage_df["stage_type"].iloc[index]
        if (stage_type != "Label") & (stage_type != "Review") & (stage_type != "Start"):
            continue

        # Create a list of answer lists
        list_answer_lists = []
        for classification_dict in classification_dict_list:
            list_answer_lists.append([])

        dict_list_answer_lists[stage_name] = list_answer_lists

    batch_name_list = []
    stage_list = []
    external_id_list = []
    task_id_list = []
    num_page_list = []
    for image_index, asset in enumerate(json_export):
        if verbose & (image_index % verbose_frequency == 0):
            logger.info("Assets Processed: [" + str(num_assets) + "/" + str(image_index) + "]")
        # --------------------------------------------------------------------------------------------------------------
        asset_url = asset['asset']
        external_id = asset['externalId']
        task = asset['task']

        batches = asset['batches']
        if None in batches:
            batches_temp = []
            for batch in batches:
                if batch != None:
                    batches_temp.append(batch)
            batches = []
            batches = batches_temp.copy()
        batches_str = '; '.join(batches)
        # --------------------------------------------------------------------------------------------------------------
        label_stage = task['stage']
        task_id = task['taskId']
        num_page = ""
        if 'dataset' in asset:
            num_page = str(len(asset['dataset']))
        if 'metadata' in asset:
            if 'frameTotal' in asset['metadata']:
                num_page = str(asset['metadata']['frameTotal'])

        raw_stage_history = []
        if 'stageHistory' in task:
            raw_stage_history = task['stageHistory']
        else:
            continue
        stage_history = filter_stage_history(raw_stage_history, stage_df)

        # Check whether the date is between start_date and end_date
        if not check_date(raw_stage_history, start_date, end_date):
            continue
        # -------------------------------
        # Consider the stages which the asset didn't enter
        for stage_index in range(len(stage_df)):
            stage_founded = False
            search_stage_id = stage_df["stage_id"].iloc[stage_index]
            search_stage_name = stage_df["stage_name"].iloc[stage_index]
            search_stage_type = stage_df["stage_type"].iloc[stage_index]

            if (search_stage_type != "Label") & (search_stage_type != "Review") & (search_stage_type != "Start"):
                continue
            for stage in stage_history:
                if stage["stageId"] == search_stage_id:
                    stage_founded = True
                    break

            if not stage_founded:
                if search_stage_type == "Label":
                    dict_stage_info[search_stage_name]['labeler_name'].append("")
                    dict_stage_info[search_stage_name]['labeling_date'].append("")
                    dict_stage_info[search_stage_name]['labeling_start_time'].append("")
                    dict_stage_info[search_stage_name]['labeling_end_time'].append("")
                    dict_stage_info[search_stage_name]['labeling_duration'].append("")
                elif search_stage_type == "Review":
                    dict_stage_info[search_stage_name]['reviewer_name'].append("")
                    dict_stage_info[search_stage_name]['review_date'].append("")
                    dict_stage_info[search_stage_name]['review_start_time'].append("")
                    dict_stage_info[search_stage_name]['review_end_time'].append("")
                    dict_stage_info[search_stage_name]['review_duration'].append("")
                    dict_stage_info[search_stage_name]['review_task_status'].append("")

                if (search_stage_type == "Label") | (search_stage_type == "Review") | (search_stage_type == "Start"):
                    dict_stage_count[search_stage_name]["number_of_classifications"].append(0)
                    dict_stage_count[search_stage_name]["number_of_tools"].append(0)
                    dict_stage_count[search_stage_name]["number_of_tool_classifications"].append(0)
                    dict_stage_count[search_stage_name]["number_of_relations"].append(0)

                num_lists = len(dict_list_answer_lists[search_stage_name])
                for ann_index in range(num_lists):
                    dict_list_answer_lists[search_stage_name][ann_index].append("")

        # -------------------------------
        for stage in stage_history:
            stage_id = stage['stageId']
            stage_name = stage['stage']
            stage_type = stage_df[stage_df["stage_id"] == stage_id]["stage_type"].iloc[0]
            if (stage_type != "Label") & (stage_type != "Review") & (stage_type != "Start"):
                continue

            stage_duration = 0
            stage_duration_sec = 0
            completed_at = ""
            completed_by = ""
            if 'duration' in stage:
                stage_duration = stage['duration']
                stage_duration_sec = stage_duration / 1000
            if 'completedAt' in stage:
                completed_at = stage['completedAt']
            if 'completedBy' in stage:
                completed_by = stage['completedBy']

            # Calculate Time Fields
            if duration_unit == 'msec':
                stage_duration_unit = str(stage_duration)
            elif duration_unit == 'sec':
                stage_duration_unit = '{0:.2f}'.format(float(stage_duration / 1000))
            elif duration_unit == 'min':
                stage_duration_unit = '{0:.2f}'.format(float(stage_duration / 60_000))
            elif duration_unit == 'hour':
                stage_duration_unit = '{0:.2f}'.format(float(stage_duration / 3_600_000))
            else:
                duration_unit = 'msec'
                stage_duration_unit = str(stage_duration)

            time_split = completed_at.split('T')
            completed_date = time_split[0]
            completed_time = time_split[1][0:8]

            year, month, day = completed_date.split('-')
            hour, minute, second = completed_time.split(':')

            end_datetime = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
            start_datetime = end_datetime - relativedelta(seconds=math.ceil(stage_duration_sec))

            date = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2)
            start_time = str(start_datetime.hour).zfill(2) + ':' + str(start_datetime.minute).zfill(2) + ':' + str(start_datetime.second).zfill(2)
            end_time = str(end_datetime.hour).zfill(2) + ':' + str(end_datetime.minute).zfill(2) + ':' + str(end_datetime.second).zfill(2)
            # ----------------------------------
            if stage_type == "Label":
                dict_stage_info[stage_name]['labeler_name'].append(completed_by)
                dict_stage_info[stage_name]['labeling_date'].append(date)
                dict_stage_info[stage_name]['labeling_start_time'].append(start_time)
                dict_stage_info[stage_name]['labeling_end_time'].append(end_time)
                dict_stage_info[stage_name]['labeling_duration'].append(stage_duration_unit)
            elif stage_type == "Review":
                dict_stage_info[stage_name]['reviewer_name'].append(completed_by)
                dict_stage_info[stage_name]['review_date'].append(date)
                dict_stage_info[stage_name]['review_start_time'].append(start_time)
                dict_stage_info[stage_name]['review_end_time'].append(end_time)
                dict_stage_info[stage_name]['review_duration'].append(stage_duration_unit)
                if 'reviewStatus' in stage:
                    dict_stage_info[stage_name]['review_task_status'].append(stage["reviewStatus"])
                else:
                    dict_stage_info[stage_name]['review_task_status'].append("")

            # ----------------------------------
            # Extract Counts
            num_classifications = 0
            if "classifications" in stage:
                classifications = stage["classifications"]

                classification_answer_dict_list = []
                scrape_answers(classifications, [], classification_answer_dict_list)
                num_classifications = len(classification_answer_dict_list)

            num_tools = 0
            num_tool_classifications = 0
            if "tools" in stage:
                tools = stage["tools"]
                num_tools = len(tools)

                for tool in tools:
                    if 'classifications' in tool:
                        tools_classifications = tool['classifications']
                        tools_classification_answer_dict_list = []
                        scrape_answers(tools_classifications, [], tools_classification_answer_dict_list)
                        num_tool_classifications = num_tool_classifications + len(tools_classification_answer_dict_list)

            num_relations = 0
            if "relations" in stage:
                relations = stage["relations"]
                num_relations = len(relations)

            dict_stage_count[stage_name]["number_of_classifications"].append(num_classifications)
            dict_stage_count[stage_name]["number_of_tools"].append(num_tools)
            dict_stage_count[stage_name]["number_of_tool_classifications"].append(num_tool_classifications)
            dict_stage_count[stage_name]["number_of_relations"].append(num_relations)

            # Add empty answers if classifications are not available
            if num_classifications == 0:
                num_lists = len(dict_list_answer_lists[stage_name])
                for ann_index in range(num_lists):
                    dict_list_answer_lists[stage_name][ann_index].append("")
                continue

            for tool_index, classification_tool in enumerate(classification_dict_list):
                found_flag = False
                schema_id = classification_tool['schemaId']
                for classification_answer in classification_answer_dict_list:
                    if schema_id == classification_answer['schemaId']:
                        answer = classification_answer['answer']
                        if answer is None:
                            continue
                        if type(answer) == list:
                            temp_list = []
                            for current_answer in answer:
                                if current_answer is not None:
                                    temp_list.append(current_answer)

                            answer = []
                            answer = temp_list.copy()
                            answer = ' | '.join(answer)
                        # list_answer_lists[tool_index].append(answer)
                        tmp_list = dict_list_answer_lists[stage_name][tool_index]
                        if len(tmp_list) == image_index:
                            tmp_list.append(answer)
                        elif len(tmp_list) > image_index:
                            tmp_list[image_index] = tmp_list[image_index] + ' | ' + answer

                        dict_list_answer_lists[stage_name][tool_index] = tmp_list
                        found_flag = True

                if not found_flag:
                    # list_answer_lists[tool_index].append('')
                    tmp_list = dict_list_answer_lists[stage_name][tool_index]
                    tmp_list.append('')
                    dict_list_answer_lists[stage_name][tool_index] = tmp_list

        batch_name_list.append(batches_str)
        stage_list.append(label_stage)
        external_id_list.append(external_id)
        task_id_list.append(task_id)
        num_page_list.append(num_page)

    logger.info("Assets Processed: [" + str(num_assets) + "/" + str(num_assets) + "]")
    data_df = pd.DataFrame()
    # data_df['Asset URL'] = asset_url_list
    data_df['Batch'] = batch_name_list
    data_df['Stage'] = stage_list
    data_df['External ID'] = external_id_list
    data_df['Task ID'] = task_id_list

    # Add stage info
    for index in range(len(stage_df)):
        stage_name = stage_df["stage_name"].iloc[index]
        stage_type = stage_df["stage_type"].iloc[index]
        if stage_type == 'Label':
            data_df["[" + stage_name + "] " + "labeler_name"] = dict_stage_info[stage_name]["labeler_name"]
            data_df["[" + stage_name + "] " + "labeling_date"] = dict_stage_info[stage_name]["labeling_date"]
            data_df["[" + stage_name + "] " + "labeling_start_time"] = dict_stage_info[stage_name]["labeling_start_time"]
            data_df["[" + stage_name + "] " + "labeling_end_time"] = dict_stage_info[stage_name]["labeling_end_time"]
            data_df["[" + stage_name + "] " + "labeling_duration (" + duration_unit + ")"] = dict_stage_info[stage_name]["labeling_duration"]
        elif stage_type == 'Review':
            data_df["[" + stage_name + "] " + "reviewer_name"] = dict_stage_info[stage_name]["reviewer_name"]
            data_df["[" + stage_name + "] " + "review_date"] = dict_stage_info[stage_name]["review_date"]
            data_df["[" + stage_name + "] " + "review_start_time"] = dict_stage_info[stage_name]["review_start_time"]
            data_df["[" + stage_name + "] " + "review_end_time"] = dict_stage_info[stage_name]["review_end_time"]
            data_df["[" + stage_name + "] " + "review_duration (" + duration_unit + ")"] = dict_stage_info[stage_name]["review_duration"]
            data_df["[" + stage_name + "] " + "review_task_status"] = dict_stage_info[stage_name]["review_task_status"]

    data_df['Number of Pages'] = num_page_list

    # Add stage count
    for index in range(len(stage_df)):
        stage_name = stage_df["stage_name"].iloc[index]
        stage_type = stage_df["stage_type"].iloc[index]

        if (stage_type == "Label") | (stage_type == "Review") | (stage_type == "Start"):
            data_df["[" + stage_name + "] " + "number_of_classifications"] = dict_stage_count[stage_name]["number_of_classifications"]
            data_df["[" + stage_name + "] " + "number_of_tools"] = dict_stage_count[stage_name]["number_of_tools"]
            data_df["[" + stage_name + "] " + "number_of_tool_classifications"] = dict_stage_count[stage_name]["number_of_tool_classifications"]
            data_df["[" + stage_name + "] " + "number_of_relations"] = dict_stage_count[stage_name]["number_of_relations"]

    for index in range(len(stage_df)):
        stage_name = stage_df["stage_name"].iloc[index]
        stage_type = stage_df["stage_type"].iloc[index]
        if (stage_type != "Label") & (stage_type != "Review") & (stage_type != "Start"):
            continue
        new_classification_dict_list = dict_list_answer_lists[stage_name]

        for tool_index, classification_tool in enumerate(new_classification_dict_list):
            schema_id = classification_dict_list[tool_index]['schemaId']
            if schema_id in ignored_schema_ids:
                continue
            category_title = classification_dict_list[tool_index]['title']
            data_df["[" + stage_name + "] " + category_title + "_id_" + schema_id] = new_classification_dict_list[tool_index]

    # ------------------------------------------------------------------------------------------------------------------
    # Calculate Metrics
    if (gold_stage is None) | (evaluation_stage is None):
        pass
    elif gold_stage not in list(stage_df['stage_name']):
        pass
    elif evaluation_stage not in list(stage_df['stage_name']):
        pass
    else:
        # total_labels_gold_stage = []
        # total_labels_evaluation_stage = []
        # matched_labels = []
        # fixed_labels = []

        total_labels_gold_stage_list = []
        total_labels_evaluate_stage_list = []
        accepted_labels_list = []
        fixed_labels_list = []

        total_precision_list = []
        total_recall_list = []
        total_accuracy_list = []
        total_count_list = []
        for asset_index in range(len(data_df)):
            total_labels_gold_stage_list.append(0)
            total_labels_evaluate_stage_list.append(0)
            accepted_labels_list.append(0)
            fixed_labels_list.append(0)
            total_precision_list.append(0)
            total_recall_list.append(0)
            total_accuracy_list.append(0)
            total_count_list.append(0)

        new_classification_dict_list = dict_list_answer_lists[gold_stage]
        for tool_index, classification_tool in enumerate(new_classification_dict_list):
            schema_id = classification_dict_list[tool_index]['schemaId']
            category_title = classification_dict_list[tool_index]['title']
            if schema_id in ignored_schema_ids:
                continue

            gold_answer = data_df["[" + gold_stage + "] " + category_title + "_id_" + schema_id]
            evaluation_answer = data_df["[" + evaluation_stage + "] " + category_title + "_id_" + schema_id]

            # Calculate Precision Recall
            for asset_index in range(len(gold_answer)):
                temp_external_id = data_df["External ID"].iloc[asset_index]
                current_gold_answer = gold_answer[asset_index]
                current_evaluation_answer = evaluation_answer[asset_index]

                if (current_gold_answer == "") & (current_evaluation_answer == ""):
                    continue

                if (current_gold_answer == "") & (current_evaluation_answer != ""):
                    current_evaluation_answer_list = current_evaluation_answer.split(" | ")
                    current_evaluation_answer_list = list(set(current_evaluation_answer_list))
                    current_evaluation_answer_list.sort()
                    total_labels_evaluate_stage_list[asset_index] = total_labels_evaluate_stage_list[asset_index] + len(current_evaluation_answer_list)

                    fixed_labels = len(current_evaluation_answer_list)
                    fixed_labels_list[asset_index] = fixed_labels_list[asset_index] + fixed_labels
                    continue

                if current_gold_answer == "":
                    current_gold_answer_list = []
                else:
                    current_gold_answer_list = current_gold_answer.split(" | ")
                    current_gold_answer_list = list(set(current_gold_answer_list))
                    current_gold_answer_list.sort()

                if current_evaluation_answer == "":
                    current_evaluation_answer_list = []
                else:
                    current_evaluation_answer_list = current_evaluation_answer.split(" | ")
                    current_evaluation_answer_list = list(set(current_evaluation_answer_list))
                    current_evaluation_answer_list.sort()

                TP = len(set(current_gold_answer_list).intersection(set(current_evaluation_answer_list)))

                precision = 0
                recall = 0
                strict_accuracy = 0
                if TP > 0:
                    precision = TP / len(current_evaluation_answer_list)
                    recall = TP / len(current_gold_answer_list)

                if (precision == 1) & (recall == 1):
                    strict_accuracy = 1

                fixed_labels = (len(current_evaluation_answer_list) - TP) + (len(current_gold_answer_list) - TP)

                accepted_labels_list[asset_index] = accepted_labels_list[asset_index] + TP
                fixed_labels_list[asset_index] = fixed_labels_list[asset_index] + fixed_labels

                total_labels_gold_stage_list[asset_index] = total_labels_gold_stage_list[asset_index] + len(current_gold_answer_list)
                total_labels_evaluate_stage_list[asset_index] = total_labels_evaluate_stage_list[asset_index] + len(current_evaluation_answer_list)

                total_precision_list[asset_index] = total_precision_list[asset_index] + precision
                total_recall_list[asset_index] = total_recall_list[asset_index] + recall
                total_accuracy_list[asset_index] = total_accuracy_list[asset_index] + strict_accuracy
                total_count_list[asset_index] = total_count_list[asset_index] + 1

            # if len(total_labels_gold_stage) == 0:
            #     total_labels_gold_stage = (gold_answer != "") * 1
            # else:
            #     total_labels_gold_stage = total_labels_gold_stage + (gold_answer != "") * 1
            #
            # if len(total_labels_evaluation_stage) == 0:
            #     total_labels_evaluation_stage = (evaluation_answer != "") * 1
            # else:
            #     total_labels_evaluation_stage = total_labels_evaluation_stage + (evaluation_answer != "") * 1

            # current_matched_labels = ( ((gold_answer != "") | (evaluation_answer != "")) & (gold_answer == evaluation_answer)) * 1
            # if len(matched_labels) == 0:
            #     matched_labels = current_matched_labels
            # else:
            #     matched_labels = matched_labels + current_matched_labels
            #
            # current_fixed_labels = (((gold_answer != "") | (evaluation_answer != "")) & (gold_answer != evaluation_answer)) * 1
            # if len(fixed_labels) == 0:
            #     fixed_labels = current_fixed_labels
            # else:
            #     fixed_labels = fixed_labels + current_fixed_labels

        # accuracy_list = 100 * matched_labels / total_labels_evaluation_stage
        # accuracy_list = accuracy_list.round(decimals=1)

        # Normalize precision and recall metrics
        for ind in range(len(total_precision_list)):
            total_count = total_count_list[ind]
            if total_count == 0:
                total_precision_list[ind] = 0
                total_recall_list[ind] = 0
                total_accuracy_list[ind] = 0
            else:
                total_precision_list[ind] = 100 * total_precision_list[ind] / total_count
                total_recall_list[ind] = 100 * total_recall_list[ind] / total_count
                total_accuracy_list[ind] = 100 * total_accuracy_list[ind] / total_count

        data_df["Gold Stage [" + gold_stage + "] Total Labels Applied"] = total_labels_gold_stage_list
        data_df["Evaluation Stage [" + evaluation_stage + "] Total Labels Applied"] = total_labels_evaluate_stage_list
        data_df["Total Labels Accepted"] = accepted_labels_list
        data_df["Total Labels Fixed"] = fixed_labels_list
        # data_df["Accuracy"] = accuracy_list
        data_df["Precision (%)"] = total_precision_list
        data_df["Recall (%)"] = total_recall_list
        data_df["Accuracy (%)"] = total_accuracy_list

        data_df["Precision (%)"] = data_df["Precision (%)"].round(decimals=1)
        data_df["Recall (%)"] = data_df["Recall (%)"].round(decimals=1)
        data_df["Accuracy (%)"] = data_df["Accuracy (%)"].round(decimals=1)
    # ------------------------------------------------------------------------------------------------------------------
    # Change Column names
    column_names = list(data_df.columns)
    for stage_index in range(len(stage_df)):
        stage_name = stage_df["stage_name"].iloc[stage_index]
        try:
            new_classification_dict_list = dict_list_answer_lists[stage_name]
        except:
            continue
        for tool_index, classification_tool in enumerate(new_classification_dict_list):
            schema_id = classification_dict_list[tool_index]['schemaId']
            category_title = classification_dict_list[tool_index]['title']

            field_name = "[" + stage_name + "] " + category_title + "_id_" + schema_id
            new_column_name = "[" + stage_name + "] " + category_title

            if field_name in column_names:
                data_df = data_df.rename(columns={field_name: new_column_name})
    # ------------------------------------------------------------------------------------------------------------------
    time_str = str(datetime.utcnow())
    time_str = time_str[:-3].replace(' ', 'T') + 'Z'
    time_str = time_str.replace(':', '-')

    output_filename = project_id + '-export-' + time_str + '_[Stage].csv'
    data_df.to_csv(output_filename, index=False)

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
                          version='v3')

    run(plugin)
