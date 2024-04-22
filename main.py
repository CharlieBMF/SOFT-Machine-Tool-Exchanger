from base import Tool
from conf import connstring
import requests
import pyodbc
import json
import datetime


def get_tool_locations(connstring):
    string = 'SELECT * FROM tToolAdr'
    cnxn = pyodbc.connect(connstring, timeout=1)
    cursor = cnxn.cursor()
    cursor.execute(string)
    result = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    del cnxn
    zipped_tool_locations = [dict(zip(columns, record)) for record in result]
    zipped_tool_locations_with_names = [{record['AdrName']: record} for record in zipped_tool_locations]
#    log_actual_tools(zipped_tool_locations_with_names)
    return zipped_tool_locations_with_names


def create_tools(tools_locations):
    tools_list = []
    for record in tools_locations:
        for k, v in record.items():
            print('K', k)
            k = Tool(location_id=v['AdrID'], location_name=v['AdrName'], location_ip=v['AdrIP'],
                     location_port=v['AdrPortNo'], location_network=v['AdrNetworkNo'],
                     location_network_plc=v['AdrNetworkPLCNo'], location_id_line=v['AdrIDLine'],
                     location_id_machine=v['AdrIDMachine'], tool_addresses=v['AdrPLC'], conn=connstring)
            tools_list.append(k)
    return tools_list


def log_actual_tools(zipped_tool_locations_with_names):
    date = str(datetime.datetime.now())
    with open('tools_construtors.txt', 'a') as file:
        file.write(date + '\n')
        for tool in zipped_tool_locations_with_names:
            file.write(json.dumps(tool) + '\n')


def log_error(e, tool):
    date = str(datetime.datetime.now())
    final_json = {"text": date + " -- " + str(tool.location_name) + " -- " + str(e)}
    response = requests.post(
        'https://hooks.slack.com/services/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',
        json=final_json
    )


tool_locations_defined_in_sql = get_tool_locations(connstring)
tools = create_tools(tool_locations_defined_in_sql)

#breaking = False
e_last = None

while True:
    for tool in tools:
        try:
            trigger, task = tool.check_trigger()
        except:
            continue
        #print('TRIGGER-TASK', trigger, task)
        if trigger:
            try:
                tool_name = tool.get_tool_name_PLC()
                operator_name = tool.get_operator_name_PLC()
                actual_counter = tool.get_actual_counter_PLC()
                print(f'tool name: {tool_name}, operator_name: {operator_name}, actual counter: {actual_counter}')
                if task == 1:
                    # DEMONTAZ
                    #print('demontaz')
                    tool_data = tool.get_tool_data_SQL(tool_name)
                    #print('1a')
                    tool.update_tool_data_SQL(tool_data, actual_counter)
                    response_writable_to_plc = tool.generate_writable_plc_response(tool_data, task, error=0)
                    #print('1')
                    print('\nresponse_writable_to_plc', response_writable_to_plc)
                    reset_readable_to_plc = tool.generate_reset_plc_signals(task, error=0)
                    #print('2')
                    print('response_readable_to_plc', reset_readable_to_plc)
                    response_to_plc = {**response_writable_to_plc, **reset_readable_to_plc}
                    #print('3')
                    print('\nGENERATED RESPONSE FOR PLC', response_to_plc)

                if task == 2:
                    # MONTAZ
                    tool_data = tool.get_tool_data_SQL(tool_name)
                    response_writable_to_plc = tool.generate_writable_plc_response(tool_data, task, error=0)
                    print('response_writable_to_plc', response_writable_to_plc)
                    reset_readable_to_plc = tool.generate_reset_plc_signals(task, error=0)
                    print('response_readable_to_plc', reset_readable_to_plc)
                    response_to_plc = {**response_writable_to_plc, **reset_readable_to_plc}
                    print('response', response_to_plc)

                tool.send_response_PLC(response_to_plc)
                tool.log_task_to_sql(task, tool_name, tool_data, operator_name, actual_counter)

            except Exception as e:
                try:
                    if e_last != str(e):
                        log_error(e, tool)
                        e_last = str(e)
                except:
                    pass
                try:
                    response_writable_to_plc = tool.generate_writable_plc_response(None, None, error=1)
                    reset_readable_to_plc = tool.generate_reset_plc_signals(None, error=1)
                    response_to_plc = {**response_writable_to_plc, **reset_readable_to_plc}
                    tool.send_response_PLC(response_to_plc)
                except:
                    pass
