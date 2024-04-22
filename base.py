import datetime
import re
import pymcprotocol
import ast
import pyodbc
import requests


class ToolLocation:
    def __init__(self, location_id, location_name, location_ip, location_port, location_network, location_network_plc,
                 location_id_line, location_id_machine):
        self.location_id = location_id
        self.location_name = location_name
        self.location_ip = location_ip
        self.location_port = location_port
        self.location_network = location_network
        self.location_network_plc = location_network_plc
        self.location_id_line = location_id_line
        self.location_id_machine = location_id_machine
        self.machine = self.define_machine_root()

    def define_machine_root(self):
        pymc3e = pymcprotocol.Type3E()
        if self.location_network and self.location_network_plc:
            pymc3e.network = self.location_network
            pymc3e.pc = self.location_network_plc
        return pymc3e

    def connect(self):
        self.machine.connect(ip=self.location_ip, port=self.location_port)

    def close_connection(self):
        self.machine.close()

    def read_bits(self, head, size=1):
        return self.machine.batchread_bitunits(headdevice=head, readsize=size)

    def read_words(self, head, size=1):
        return self.machine.batchread_wordunits(headdevice=head, readsize=size)

    def read_random_words(self, word_devices, double_word_devices):
        return self.machine.randomread(word_devices=word_devices, dword_devices=double_word_devices)

    def write_word(self, head, values):
        self.machine.batchwrite_wordunits(headdevice=head, values=values)

    def write_random_bits(self, bits: list, values: list):
        self.machine.randomwrite_bitunits(bit_devices=bits, values=values)

    def write_random_words(self, word_devices: list, word_values: list,
                           double_word_devices: list, double_word_values: list):
        self.machine.randomwrite(word_devices=word_devices, word_values=word_values, dword_devices=double_word_devices,
                                 dword_values=double_word_values)


class Tool(ToolLocation):
    def __init__(self, location_id, location_name, location_ip, location_port, location_network, location_network_plc,
                 location_id_line, location_id_machine, tool_addresses, conn):

        super().__init__(location_id, location_name, location_ip, location_port, location_network, location_network_plc,
                         location_id_line, location_id_machine)
        self.tool_addresses = ast.literal_eval(tool_addresses)
        self.conn_string = conn

    def check_possibility_to_use_at_location(self, tool_data):
        possible_locations = self.get_possible_locations_SQL(tool_data)
        #print(possible_locations)
        if self.location_id in possible_locations:
            return True
        else:
            return False


    def check_trigger(self):
        self.connect()
        response_trigger = self.read_bits(head=self.tool_addresses['R_Task_Request_M'])
        response_task = self.read_words(head=self.tool_addresses['R_Task_No_W'])
        self.close_connection()
        return response_trigger[0], response_task[0]

    def get_tool_name_PLC(self):
        self.connect()
        tool_name_decimal = (
            self.read_words(self.tool_addresses['R_Tool_Code_ASCII'][0], len(self.tool_addresses['R_Tool_Code_ASCII'])))
        self.close_connection()
        tool_name_ascii = self.convert_to_ascii(tool_name_decimal)
        return tool_name_ascii

    def get_operator_name_PLC(self):
        self.connect()
        operator_name_decimal = (
            self.read_words(self.tool_addresses['R_Operator_ASCII'][0], len(self.tool_addresses['R_Operator_ASCII'])))
        self.close_connection()
        operator_name_ascii = self.convert_to_ascii(operator_name_decimal)
        return operator_name_ascii

    def get_actual_counter_PLC(self):
        self.connect()
        actual_counter = self.read_random_words(
            word_devices=[], double_word_devices=self.tool_addresses['R_Actual_Counter_DW'])
        self.close_connection()
        return actual_counter[1][0]

    def get_tool_data_SQL(self, tool_name):
        query = (f"SELECT DefID, TypeName, DefToolID, DefReplacementQty, DefProducedQty, DefConfirmation, DefRelease, "
                 f"StatsProdAllowed, StatsDescription, DefToolType "
                 f"FROM vToolDefinitionFullView "
                 f"WHERE DefToolID = '{tool_name}'")
        tool_data = self.execute_query(query, 'SELECT', 'one')
        #print('Tool data ', tool_data)
        return tool_data[0]

    def get_possible_locations_SQL(self, tool_data):
        query = (f"SELECT idAdr "
                 f"FROM tToolTypesAdresses "
                 f"WHERE idType = {tool_data['DefToolType']}")
        locations_kv = self.execute_query(query, 'SELECT', 'many')
        locations = [entry['idAdr'] for entry in locations_kv]
        return locations

    def update_tool_data_SQL(self, tool_data, actual_counter):
        current_datetime = datetime.datetime.now()
        DefLastCounterUpdate = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        query = (f'UPDATE tToolDefinitions SET DefProducedQty = {actual_counter}, '
                 f'DefLastCounterUpdate = \'{DefLastCounterUpdate}\' '
                 f'WHERE DefID = {tool_data["DefID"]}')
        self.execute_query(query, 'UPDATE')

    def generate_writable_plc_response(self, tool_data, task, error):
        if error == 0:
            if task == 1:
                response = self.generate_ok_response(tool_data, task)
            if task == 2:
                possible_to_use_at_this_location = self.check_possibility_to_use_at_location(tool_data)
                if (int(tool_data['DefProducedQty']) < int(tool_data['DefReplacementQty'])
                        and tool_data['DefConfirmation']
                        and tool_data['DefRelease']
                        and tool_data['StatsProdAllowed']
                        and possible_to_use_at_this_location):
                    # TOOL IS OK AND CAN BE USED TO PRODUCTION
                    # print('ALLOK')
                    response = self.generate_ok_response(tool_data, task)

                else:
                    response = self.generate_ng_response(tool_data, task)
        if error == 1:
            response = self.generate_error_response()
        return response

    def generate_ok_response(self, tool_data, task):
        keys = [writable for writable in self.tool_addresses.keys() if writable.startswith('W')]
        if task == 1:
            values = [1, 1, 0, 0, 0, 0, 0]
        if task == 2:
            values = [1, 1, 0, 1, 0, int(tool_data['DefProducedQty']), int(tool_data['DefReplacementQty'])]
        response = dict(zip(keys, values))
        return response

    def generate_ng_response(self, tool_data, task):
        if task == 2:
            keys = [writable for writable in self.tool_addresses.keys() if writable.startswith('W')]
            values = [1, 1, 0, 0, 1, int(tool_data['DefProducedQty']), int(tool_data['DefReplacementQty'])]
            response = dict(zip(keys, values))
            return response

    def generate_error_response(self):
        keys = [writable for writable in self.tool_addresses.keys() if writable.startswith('W')]
        values = [1, 0, 1, 0, 0, 0, 0]
        response = dict(zip(keys, values))
        return response

    def generate_reset_plc_signals(self, task, error):
        if error == 0:
            if task == 1:
                reseter = {
                    'R_Task_Request_M': 0,
    #                'R_Task_No_W': 0,
                    'R_Tool_Code_ASCII': [0] * len(self.tool_addresses['R_Tool_Code_ASCII']),
    #                'R_Actual_Counter_DW': [0],
    #                'R_Operator_ASCII': [0] * len(self.tool_addresses['R_Operator_ASCII'])
                }
                # print('\nRESET FOR DISASSEMBLY', reseter)
            if task == 2:
                reseter = {'R_Task_Request_M': 0,
    #                       'R_Task_No_W': 0,
    #                       'R_Operator_ASCII': [0] * len(self.tool_addresses['R_Operator_ASCII'])
                           }
                # print('RESET FOR ASSEMBLY', reseter)
        if error == 1:
            reseter = {'R_Task_Request_M': 0}
        return reseter

    def send_response_PLC(self, response):
        markers_addresses, markers_values = self.separate_response(response, 'marker')
        words_addresses, words_values = self.separate_response(response, 'word')
        dwords_addresses, dwords_values = self.separate_response(response, 'dword')
        # print(markers_addresses, markers_values)
        # print(words_addresses, words_values)
        # print(dwords_addresses, dwords_values)
        self.connect()
        self.write_random_bits(markers_addresses, markers_values)
        self.write_random_words(word_devices=words_addresses, word_values=words_values,
                                double_word_devices=dwords_addresses, double_word_values=dwords_values)
        self.close_connection()

    def separate_response(self, response, type_of_separation):
        # print('\n\nRESPONSE', response, type_of_separation)
        if type_of_separation == 'marker':
            separated = {key: value for key, value in response.items() if key.endswith('M')}
            separated_keys = list({self.tool_addresses.get(key, key): value for key, value in separated.items()}.keys())
            separated_values = (
                list({self.tool_addresses.get(key, key): value for key, value in separated.items()}.values()))
        else:
            if type_of_separation == 'word':
                separated = \
                    {key: value for key, value in response.items() if key.endswith('_W') or key.endswith('ASCII')}
            if type_of_separation == 'dword':
                separated = {key: value for key, value in response.items() if key.endswith('DW')}
            separated_keys = [self.tool_addresses[key] for key in separated.keys()]
            separated_keys = [item if not isinstance(sublist, list) else item for sublist in separated_keys for
                              item in (sublist if isinstance(sublist, list) else [sublist])]
            separated_values = [separated[key] for key in separated.keys()]
            separated_values = [item if not isinstance(sublist, list) else item for sublist in separated_values for
                                item in (sublist if isinstance(sublist, list) else [sublist])]
        # print(type_of_separation, separated_keys, separated_values)
        return separated_keys, separated_values

    def log_task_to_sql(self, task, tool_name, tool_data, operator_name, actual_counter):
        LogToolID = tool_data['DefID']
        LogLineId = self.location_id_line
        LogLineMachineID = self.location_id_machine
        current_datetime = datetime.datetime.now()
        LogDate = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        LogUserLogin = operator_name

        if task == 1:
            LogType = 4
            LogDescription = (
                f'\'{tool_name} | '
                f'RepQty: {tool_data["DefReplacementQty"]} | '
                f'ProdQty: {actual_counter} | '
                f'Confirmation: {tool_data["DefConfirmation"]} | '
                f'Release: {tool_data["DefRelease"]} | '
                f'Status: {tool_data["StatsDescription"]}\''
            )

        if task == 2:
            LogType = 3
            LogDescription = (
                f'\'{tool_name} | '
                f'RepQty: {tool_data["DefReplacementQty"]} | '
                f'ProdQty: {tool_data["DefProducedQty"]} | '
                f'Confirmation: {tool_data["DefConfirmation"]} | '
                f'Release: {tool_data["DefRelease"]} | '
                f'Status: {tool_data["StatsDescription"]}\''
            )

        query = (f'INSERT INTO tToolLogs (LogToolID, LogLineId, LogMachineID, LogDate, LogType, LogDescription, '
                 f'LogUserLogin) VALUES ({LogToolID}, {LogLineId}, {LogLineMachineID}, \'{LogDate}\', {LogType}, '
                 f'{LogDescription}, \'{LogUserLogin}\')')
        self.execute_query(query, 'INSERT')

    def execute_query(self, query, type_of_query, range_of_query=None):
        if type_of_query == 'SELECT':
            result = self.cursor_execution(query, type_of_query, range_of_query)
            return result
        if type_of_query == 'INSERT' or type_of_query == 'UPDATE':
            self.cursor_execution(query, type_of_query, range_of_query)

    def cursor_execution(self, query, type_of_query, range_of_query):
        print('\n', query, '\n', type_of_query, range_of_query)
        self.log_query_to_slack(query)
        cnxn = pyodbc.connect(self.conn_string, timeout=1)
        cursor = cnxn.cursor()
        cursor.execute(query)

        if type_of_query == 'SELECT':
            columns = [column[0] for column in cursor.description]
            if range_of_query == 'one':
                result = cursor.fetchone()
                if result is None:
                    raise Exception(f'Response None from SQL for fetch tool query: {query}')
                zipped = [dict(zip(columns, result))]
            if range_of_query == 'many':
                result = cursor.fetchall()
                if not result:
                    raise Exception(f'Response None from SQL for fetch tool query: {query}')
                # Assuming columns is a list of column names
                zipped = [dict(zip(columns, row)) for row in result]

        if type_of_query == 'INSERT' or type_of_query == 'UPDATE':
            cursor.commit()
            zipped = None

        del cnxn
        return zipped

    def log_query_to_slack(self, query):
        date = str(datetime.datetime.now())
        final_json = {"text": date + " -- " + str(self.location_name) + " -- " + query}
        response = requests.post(
            'https://hooks.slack.com/services/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',
            json=final_json
        )

    @staticmethod
    def convert_to_ascii(decimal_list):
        binary = [bin(b).replace('0b', '').zfill(16) for b in decimal_list]
        halves_list = [half for word in binary for half in (word[len(word) // 2:], word[:len(word) // 2])]
        ascii_string = ''.join([chr(int(binary, 2)) for binary in halves_list])
        ascii_string_result = re.sub(r'[^a-zA-Z0-9/-]', '', ascii_string)
        return ascii_string_result






