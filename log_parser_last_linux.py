import re
import os
import logging
from collections import defaultdict

logging.basicConfig(filename='log_parser.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

logging.info(f"STARTING PROCESSING THE LOG FILES")

def generate_output_filename(input_filepath, suffix="_stats"):

    #Генерация имени файла для файла статистики
    try:
        file_name, file_ext = os.path.splitext(os.path.basename(input_filepath))
        output_file_name = f"{file_name}{suffix}{file_ext}"
        directory = os.path.dirname(input_filepath)
        output_filepath = os.path.join(directory, output_file_name)

        return output_filepath
    except Exception as e:
        logging.error(f"Error generating output filename for {input_filepath}: {e}")
        return None

def generate_comparison_filename(input_filepath_1, input_filepath_2, suffix="_intersection"):

    #Имя файла сравнения
    try:
        file_name_1, file_ext_1 = os.path.splitext(os.path.basename(input_filepath_1))
        file_name_2, file_ext_2 = os.path.splitext(os.path.basename(input_filepath_2))


        output_file_name = f"{file_name_1} + {file_name_2}{suffix}{file_ext_1}"
        directory = os.path.dirname(input_filepath_1)
        output_filepath = os.path.join(directory, output_file_name)

        return output_filepath
    
    except Exception as e:
        logging.error(f"Error generating comparison filename for {input_filepath_1} and {input_filepath_2}: {e}")
        return None

class log:

    #Класс для log entry

    def event_counter(filepath):

        #Подсчёт событий Stop и Start в одном файле
        try:
            username_pattern = r'[a-zA-Z]{3}\s\d{2}\s\d{4}\s\d{2}:\d{2}:\d{2}\s\+\d{2},([^\s,]+),'
            event_pattern = r',(Stop|Start),'
            results = {}
            output = []

            if not os.path.exists(filepath):
                logging.error(f"File does not exist: {filepath}")
                return ""

            with open(filepath, encoding='utf-8') as file:
                logs = file.read().splitlines()


            for log in logs:
                username_match = re.search(username_pattern, log)
                event_match = re.search(event_pattern, log)


                if username_match and event_match:
                    username = username_match.group(1)
                    event = event_match.group(1)
                    if username not in results:
                        results[username] = {'Start': 0, 'Stop': 0}
                    results[username][event] += 1


            for username, event_counts in results.items():
                total_events = event_counts['Start'] + event_counts['Stop']
                output.append(f"Username: {username}, Total: {total_events}, Start: {event_counts['Start']}, Stop: {event_counts['Stop']}\n")


            return ''.join(output)
        
        except Exception as e:
            logging.error(f"Error processing event counter for {filepath}: {e}")
            return ""

    def usernames(filepath):

        #Выгрузка только имён пользователей
        try:
            usernames = []
            username_pattern = r'[a-zA-Z]{3}\s\d{2}\s\d{4}\s\d{2}:\d{2}:\d{2}\s\+\d{2},([^\s,]+),'

            if not os.path.exists(filepath):
                logging.error(f"File does not exist: {filepath}")
                return []
        
            with open(filepath, encoding='utf-8') as file:
                logs = file.read().splitlines()


            for log in logs:
                username_match = re.search(username_pattern, log).group(1)
                if username_match:
                    usernames.append(username_match)
                else:
                    logging.warning(f"Username pattern not found in log line: {log}")

            return(usernames)

        except Exception as e:
            logging.error(f"Error processing usernames for {filepath}: {e}")
            return []

    def logs_merge(filepath_1, filepath_2):

        #Объединение двух файлов в один и сортировка по времени
        try:
            username_pattern = r'[a-zA-Z]{3}\s\d{2}\s\d{4}\s\d{2}:\d{2}:\d{2}\s\+\d{2},([^\s,]+),'
            timestamp_pattern = r'([a-zA-Z]{3}\s\d{2}\s\d{4}\s\d{2}:\d{2}:\d{2})'
            event_pattern = r',(Stop|Start),'
            session_id_pattern = r'(?:Stop|Start),(\d+)'
            result = []
            result_1 = []
            result_2 = []
            file_1_name = os.path.splitext(os.path.basename(filepath_1))
            file_2_name = os.path.splitext(os.path.basename(filepath_2))

            if not os.path.exists(filepath_1) or not os.path.exists(filepath_2):
                logging.error(f"One or both files do not exist: {filepath_1}, {filepath_2}")
                return []

            with open(filepath_1, encoding='utf-8') as file:
                logs_1 = file.read().splitlines()


            with open(filepath_2, encoding='utf-8') as file:
                logs_2 = file.read().splitlines()


            for log in logs_1:
                try:
                    username = re.search(username_pattern, log).group(1)
                    timestamp = re.search(timestamp_pattern, log).group(1)
                    event = re.search(event_pattern, log).group(1)
                    session_id = re.search(session_id_pattern, log).group(1)

                    if 'alma' in file_1_name[0]:
                        node = 'alma'
                    if 'asta' in file_1_name[0]:
                        node = 'asta'

                    result_1.append(f"Timestamp: {timestamp}, Username: {username}, Event: {event}, SessionID: {session_id}, Node: {node}")
                except AttributeError:
                    logging.warning(f"Malformed log entry in {filepath_1}: {log}")

            for log in logs_2:
                try:
                    username = re.search(username_pattern, log).group(1)
                    timestamp = re.search(timestamp_pattern, log).group(1)
                    event = re.search(event_pattern, log).group(1)
                    session_id = re.search(session_id_pattern, log).group(1)

                    if 'alma' in file_2_name[0]:
                        node = 'alma'
                    if 'asta' in file_2_name[0]:
                        node = 'asta'

                    result_2.append(f"Timestamp: {timestamp}, Username: {username}, Event: {event}, SessionID: {session_id}, Node: {node}")
                except AttributeError:
                    logging.warning(f"Malformed log entry in {filepath_2}: {log}")

            result = result_1 + result_2
            result.sort()

            return result
        
        except Exception as e:
            logging.error(f"Error merging logs: {e}")
            return []

    def group_logs(merged_logs):

        #Группировка логов по юзернейму
        try:
            logs_by_user = defaultdict(list) #пустой словарь для группировки событий по юзернейму
            sessions_by_user = defaultdict(dict) #пустой словарь для группировки сессий для каждого юзернейма

            timestamp_pattern = r'Timestamp: ([^,]+)'
            username_pattern = r'Username: ([^,]+)'
            event_pattern = r'Event: ([^,]+)'
            session_id_pattern = r'SessionID: (\d+)'
            node_pattern = r'Node: ([^,]+)'
            
            #читаем агрегированный лог
            for log in merged_logs:
                
                timestamp_match = re.search(timestamp_pattern, log)
                username_match = re.search(username_pattern, log)
                event_match = re.search(event_pattern, log)
                session_id_match = re.search(session_id_pattern, log)
                node_match = re.search(node_pattern, log)

                if timestamp_match and username_match and event_match and session_id_match and node_match:
                
                #каждому юзеру присваеваем поля
                    timestamp = timestamp_match.group(1)
                    username = username_match.group(1)
                    event = event_match.group(1)
                    session_id = session_id_match.group(1)
                    node = node_match.group(1)

                    logs_by_user[username].append({
                        "timestamp": timestamp,
                        "event": event,
                        "sessionID": session_id,
                        "node": node
                    })
                    
                    
                    # Группировка по сессиям
                    if session_id not in sessions_by_user[username]:
                        sessions_by_user[username][session_id] = {}

                    sessions_by_user[username][session_id][event] = {
                        'timestamp': timestamp,
                        'node': node
                    }

            #считаем незаконченные сессии (нет Stop)
            unfinished_sessions = {}

            start_count = 0
            stop_count = 0
            
            for username, sessions in sessions_by_user.items():
                for session_id, events in sessions.items():
                    
                    if 'Start' in events:
                        start_count += 1
                    if 'Stop' in events:
                        stop_count += 1
                    # Проверяем, если в сессии есть Start, но нет Stop
                    if 'Start' in events and 'Stop' not in events:
                        if username not in unfinished_sessions:
                            unfinished_sessions[username] = []
                        unfinished_sessions[username].append({
                            "sessionID": session_id,
                            "start_time": events['Start']['timestamp'],
                            "start_node": events['Start']['node']
                        })
            total_event_count = start_count + stop_count
            stats = f"Total {total_event_count} number of events have been calculated, including {start_count} of 'Start', {stop_count} of 'Stop'"

            #Вывод дублированных событий (два Start/Stop в одной сессии):

            duplicates = defaultdict(lambda: {'username': None, 'starts': [], 'stops': []})

            for username, sessions in logs_by_user.items():
                for session in sessions:
                    session_id = session['sessionID']
                    event = session['event']

                    if session_id not in duplicates:
                        duplicates[session_id] = {
                            'username': username,
                            'starts': [],
                            'stops': []
                        }

                    if event == 'Start':
                        duplicates[session_id]['starts'].append(session)
                    elif event == 'Stop':
                        duplicates[session_id]['stops'].append(session)
            duplicate_starts = {sid: info for sid, info in duplicates.items() if len(info['starts']) > 1}
            duplicate_stops = {sid: info for sid, info in duplicates.items() if len(info['stops']) > 1}

            return logs_by_user, unfinished_sessions, duplicate_starts, duplicate_stops, stats
        
        except Exception as e:
            logging.error(f"Error grouping logs: {e}")
            return {}

#Функции для записи в файлы:

def write_sessions_to_file(filepath_1, filepath_2, output_filepath):

    #Функция для вывода сгруппированных по юзернейму сессий в файл
    try:

        merged_logs = log.logs_merge(filepath_1,filepath_2)
        sessions_by_user, *any = log.group_logs(merged_logs)
        sessions_count = 0

        if sessions_by_user:

            with open(output_filepath, 'w', encoding='utf-8') as file:
                file.write("All Sessions by User:\n")
                

                for username, sessions in sessions_by_user.items():
                    file.write(f"Username: {username}\n")
                    for session in sessions:

                        session_id = session['sessionID']
                        node = session['node']
                        if session.get('event') == 'Start':
                            start_time = session.get('timestamp', 'N/A')
                            file.write(f"\tSession ID: {session_id}, Start: {start_time}, Node: {node}\n")
                        elif session.get('event') == 'Stop':
                            stop_time = session.get('timestamp', 'N/A')
                            file.write(f"\tSession ID: {session_id}, Stop: {stop_time}, Node: {node}\n")
                    sessions_count += 1

            logging.info(f"Successfully wrote {sessions_count} sessions to {output_filepath}")
        else:
            logging.info(f"No sessions for {filepath_1} and {filepath_2}")

    except Exception as e:
        logging.error(f"Error writing sessions sessions to {output_filepath}: {e}")
        return []

def write_merged_log(filepath_1, filepath_2 , output_filepath):

    #Функция для вывода объединённого лога в файл
    try:
        agg_log = log.logs_merge(filepath_1,filepath_2)

        if agg_log:
            with open(output_filepath, 'w', encoding='utf-8') as file:
                for item in agg_log:
                    file.write("%s\n" % item)
            
            logging.info(f"Successfully wrote aggregated log file to {output_filepath}")

        else:
            logging.info(f"No logs for {filepath_1} and {filepath_2}")
    
    except Exception as e:
        logging.error(f"Error writing merged log to {output_filepath}: {e}")

def write_unfinished_sessions(filepath_1, filepath_2 , output_filepath):

    #Функция для вывода неоконченных сессий в файл
    try:
        merged_logs = log.logs_merge(filepath_1,filepath_2)
        sessions_by_user, unfinished_sessions, *any, stats = log.group_logs(merged_logs)
        unfinished_sessions_count = 0

        if unfinished_sessions:

            with open(output_filepath, 'w', encoding='utf-8') as file:
                file.write("Unfinished Sessions:\n")
                
                for username, sessions in unfinished_sessions.items():
                    file.write(f"Username: {username}\n")
                    for session in sessions: 
                        session_id = session['sessionID']
                        start_time = session['start_time']
                        start_node = session['start_node']

                        file.write(f"\tSession ID: {session_id}, Start: {start_time}, Node: {start_node}\n")
                        unfinished_sessions_count += 1
            
            logging.info(f"Successfully wrote {unfinished_sessions_count} unfinished sessions to {output_filepath}")  
            logging.info(f"{stats}")
        else:
            logging.info(f"No unfinished sessions for {filepath_1} and {filepath_2}")
    
    except Exception as e:
        logging.error(f"Error writing unfinished sessions to {output_filepath}: {e}")

def write_stats(filepath, output_filepath):

    #Функция для записи файлов со статистикой
    try:
        stats_data = log.event_counter(filepath)
        if stats_data:
            with open(output_filepath, 'w', encoding='utf-8') as file:
                file.write(stats_data)
            logging.info(f"Successfully wrote stats to {output_filepath}")
        else:
            logging.warning(f"No stats generated for {filepath}")
    except Exception as e:
        logging.error(f"Error writing stats to {output_filepath}: {e}")

def write_duplicates(filepath_1, filepath_2, output_filepath):

    #Функция для записи файла с дублирующимися событиями Start/Stop
    try:
        merged_logs = log.logs_merge(filepath_1,filepath_2)
        *any, duplicate_starts, duplicate_stops, stats = log.group_logs(merged_logs)

        total_starts = sum(int(len(info['starts'])/2) for info in duplicate_starts.values())
        total_stops = sum(int(len(info['stops'])/2) for info in duplicate_stops.values())
        total_duplicates = total_starts + total_stops

        if duplicate_starts or duplicate_stops:
            with open(output_filepath, 'w', encoding='utf-8') as file:
                file.write(f"Duplicated Starts (Total {total_starts}):\n")

                for sid, info in duplicate_starts.items():
                    file.write(f"\tSession ID: {sid}, Username: {info['username']}, Count: {len(info['starts'])}\n")
                    for start in info['starts']:
                        file.write(f"\tStart: {start['timestamp']}, Node: {start['node']}\n")

                file.write("\n")

                file.write(f"Duplicated Stops (Total {total_stops}):\n")
                for sid, info in duplicate_stops.items():
                    file.write(f"\tSession ID: {sid}, Username: {info['username']}, Count: {len(info['stops'])}\n")
                    for stop in info['stops']:
                        file.write(f"\tStop: {stop['timestamp']}, Node: {stop['node']}\n")
                
                file.write("\n")

                file.write(f"Total number of duplicated events: {total_duplicates}\n")

            logging.info(f"{total_duplicates} duplicated events have been successfully found for {filepath_1} and {filepath_2}, see {output_filepath}")
        else:
            logging.info(f"No duplicated events for {filepath_1} and {filepath_2}")
    
    except Exception as e:
        logging.error(f"Error writing duplicates to {output_filepath}: {e}")

input_file_1 = "alma_out_09.16_acc.txt"
input_file_2 = "asta_out_09.16_acc.txt"
merged_output_filepath = "asta+alma.txt"
sessions_filepath = "sessions.txt"
unfinished_sessions_filepath = "unfinished_sessions.txt"
duplicated_events_filepath = "duplicates.txt"
stats_output_file_1 = generate_output_filename(input_file_1)
stats_output_file_2 = generate_output_filename(input_file_2)


write_stats(input_file_1,stats_output_file_1)
write_stats(input_file_2,stats_output_file_2)
write_merged_log(input_file_1, input_file_2, merged_output_filepath)
write_duplicates (input_file_1, input_file_2, duplicated_events_filepath)
write_sessions_to_file(input_file_1, input_file_2, sessions_filepath)
write_unfinished_sessions(input_file_1, input_file_2, unfinished_sessions_filepath)

logging.info(f"FINISHING PROCESSING THE LOG FILES")
