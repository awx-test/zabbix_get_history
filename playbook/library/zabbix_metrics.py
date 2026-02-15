#!/usr/bin/python

from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r'''
---
module: zabbix_metrics
short_description: Collect metrics from Zabbix for specified hosts
description:
  - This module collects CPU, memory, disk, and network metrics from Zabbix
  - Returns metrics for working hours (9:00-18:00) over specified period
  - Generates Excel report with collected metrics
version_added: "1.0.0"
author: "Your Name"
options:
  zabbix_server:
    description: URL of Zabbix server
    required: true
    type: str
  username:
    description: Zabbix username
    required: true
    type: str
  password:
    description: Zabbix password
    required: true
    type: str
    no_log: true
  host_names:
    description: List of host names in Zabbix
    required: true
    type: list
    elements: str
  days_back:
    description: Number of days to look back
    required: false
    type: int
    default: 31
  timezone:
    description: Timezone for working hours
    required: false
    type: str
    default: "Asia/Yekaterinburg"
  output_path:
    description: Path to save Excel report
    required: false
    type: str
    default: "/tmp/server_metrics.xlsx"
requirements:
  - zabbix-utils
  - pandas
  - openpyxl
  - pytz
'''

EXAMPLES = r'''
- name: Collect Zabbix metrics
  zabbix_metrics:
    zabbix_server: "zbx.whorse.ru/zabbix"
    username: "kvg"
    password: "{{ zabbix_password }}"
    host_names:
      - "KDC (192.168.8.3)"
    days_back: 31
    output_path: "/tmp/server_metrics.xlsx"
'''

RETURN = r'''
excel_file:
  description: Path to generated Excel file
  type: str
  returned: always
  sample: "/tmp/server_metrics.xlsx"
metrics:
  description: Collected metrics data
  type: list
  returned: success
  sample: [
    ["KDC (192.168.8.3)", "CPU utilization", "%", 15.5, 25.3, 45.1]
  ]
'''

import re
import os
from datetime import datetime, timedelta, time, timezone

from ansible.module_utils.basic import AnsibleModule
import pandas as pd
import pytz

# Проверка наличия zabbix-utils
try:
    from zabbix_utils import ZabbixAPI

    HAS_ZABBIX = True
except ImportError:
    HAS_ZABBIX = False

# Константы
KEYS_TEMPLATE = [
    "system.cpu.util",
    "vm.memory.util",
    'perf_counter_en["\\PhysicalDisk(0 C:)\\% Idle Time",60]',
    'net.if.in',
    'net.if.out'
]

DATA_COLUMNS = ["Server", "Type", "unit measurements", "Min", "Avg", "Max"]


def bps_to_mbps(bps):
    """Конвертирует биты в секунду в мегабиты в секунду"""
    return bps / 1_000_000


def change_utc2timestamp(utc_time):
    """Конвертирует UTC время в timestamp"""
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = utc_time - epoch
    return delta.total_seconds()


def get_timestamp_ekb2timestamp(timezone_str, days_back=0):
    """Получение временных меток для рабочего времени"""
    ekb_tz = pytz.timezone(timezone_str)
    utc_tz = pytz.UTC

    now_ekb = datetime.now(ekb_tz)
    if now_ekb.time() < time(9, 0):
        base_date = now_ekb.date() - timedelta(days=1)
    else:
        base_date = now_ekb.date()

    result = []

    for i in range(days_back + 1):
        target_date = base_date - timedelta(days=i)

        start_ekb = ekb_tz.localize(datetime.combine(target_date, time(9, 0)))
        end_ekb = ekb_tz.localize(datetime.combine(target_date, time(18, 0)))

        start_utc = start_ekb.astimezone(utc_tz)
        end_utc = end_ekb.astimezone(utc_tz)

        result.append({
            'date': target_date.strftime('%Y-%m-%d'),
            'time_from': int(change_utc2timestamp(start_utc)),
            'time_till': int(change_utc2timestamp(end_utc)),
            'readable_utc_start': start_utc.strftime('%Y-%m-%d %H:%M:%S UTC')
        })

    return result


def connect_zabbix(server, username, password):
    """Подключение к Zabbix API"""
    zapi = ZabbixAPI(url=server)
    zapi.login(user=username, password=password)
    return zapi


def get_host_id(zapi, host_name):
    """Получение ID хоста по имени"""
    hosts = zapi.host.get(
        filter={'name': host_name},
        output=['hostid', 'host']
    )

    if not hosts:
        raise Exception(f"Хост '{host_name}' не найден")

    return hosts[0]['hostid'], hosts[0]['host']


def get_item(zapi, host_id, item_key):
    """Получение элемента по ключу"""
    return zapi.item.get({
        "hostids": host_id,
        "search": {"key_": item_key},
        "output": ["itemid", "name", "key_"]
    })


def collect_host_metrics(zapi, host_name, days_back, timezone_str):
    """Сбор метрик для конкретного хоста"""
    keys = []
    data = []

    # Получение ID хоста
    host_id, host_real_name = get_host_id(zapi, host_name)

    # Получение всех элементов хоста
    all_items = zapi.item.get({
        "hostids": host_id,
        "output": ["itemid", "name", "key_", "units"],
        "searchWildcardsEnabled": True,
        "filter": {"status": 0}
    })

    # Поиск нужных ключей
    for item in all_items:
        for key in KEYS_TEMPLATE:
            if key in item['key_']:
                keys.append(item['key_'])

    # Получение временных диапазонов
    day_ranges = get_timestamp_ekb2timestamp(timezone_str, days_back)

    # Получение элементов для сбора
    items = []
    for key in keys:
        item_result = get_item(zapi, host_id, key)
        if item_result:
            items.append(item_result[0])

    if not items:
        return data

    # Инициализация структуры для хранения данных
    for item in items:
        item['host'] = {'hostid': host_id, 'host_name': host_real_name, 'host_vname': host_name}
        item['list_of_days'] = []
        item['list_of_history_for_total'] = []
        item['list_of_total'] = []

    # Сбор истории по дням
    for day_range in day_ranges:
        for item in items:
            history = zapi.history.get({
                "itemids": item["itemid"],
                "time_from": day_range['time_from'],
                "time_till": day_range['time_till'],
                "output": ["clock", "value"],
                "history": 3 if "net.if." in item['key_'] else 0,
                "sortfield": "clock",
                "sortorder": "DESC"
            })

            if history:
                values = [float(h["value"]) for h in history]
                if len(values) > 1:
                    avg = sum(values) / len(values)
                    max_value = max(values)
                    min_value = min(values)

                    item['list_of_days'].append({
                        "date": day_range['date'],
                        "itemid": item["name"],
                        "avg": float(f"{avg:.1f}"),
                        "min": float(f"{min_value:.1f}"),
                        "max": float(f"{max_value:.1f}"),
                        "history_count": len(history),
                    })

                item['list_of_history_for_total'].extend(history)

    # Расчет общих значений
    for item in items:
        values = [float(h["value"]) for h in item['list_of_history_for_total']]
        if len(values) > 1:
            avg = sum(values) / len(values)
            max_value = max(values)
            min_value = min(values)

            item['list_of_total'].append({
                "itemid": item["name"],
                "avg": float(f"{avg:.1f}"),
                "min": float(f"{min_value:.1f}"),
                "max": float(f"{max_value:.1f}"),
                "history_count": len(item['list_of_history_for_total']),
            })

    # Формирование выходных данных
    for item in items:
        if item.get('list_of_total') and len(item['list_of_total']) > 0:
            min_ = item['list_of_total'][0]['min']
            avg_ = item['list_of_total'][0]['avg']
            max_ = item['list_of_total'][0]['max']

            if re.search(r'Bits (received|sent)', item['name']):
                unit_measurements = 'Mbps'
            elif re.search(r'(Disk|CPU|Memory) utilization', item['name']):
                unit_measurements = '%'
            else:
                unit_measurements = 'count'

            # Конвертация для сетевых метрик
            min_val = min_ if not re.search(r'Bits (received|sent)', item['name']) else bps_to_mbps(min_)
            avg_val = avg_ if not re.search(r'Bits (received|sent)', item['name']) else bps_to_mbps(avg_)
            max_val = max_ if not re.search(r'Bits (received|sent)', item['name']) else bps_to_mbps(max_)

            data.append([
                item['host']['host_vname'],
                item['name'],
                unit_measurements,
                min_val,
                avg_val,
                max_val
            ])

    return data


def main():
    """Основная функция модуля"""
    module = AnsibleModule(
        argument_spec=dict(
            zabbix_server=dict(type='str', required=True),
            username=dict(type='str', required=True),
            password=dict(type='str', required=True, no_log=True),
            host_names=dict(type='list', elements='str', required=True),
            days_back=dict(type='int', required=False, default=31),
            timezone=dict(type='str', required=False, default='Asia/Yekaterinburg'),
            output_path=dict(type='str', required=False, default='/tmp/server_metrics.xlsx'),
        ),
        supports_check_mode=False
    )

    # Проверка наличия необходимых библиотек
    if not HAS_ZABBIX:
        module.fail_json(
            msg='The zabbix-utils library is required for this module'
        )

    try:
        # Получение параметров
        zabbix_server = module.params['zabbix_server']
        username = module.params['username']
        password = module.params['password']
        host_names = module.params['host_names']
        days_back = module.params['days_back']
        timezone_str = module.params['timezone']
        output_path = module.params['output_path']

        # Подключение к Zabbix
        zapi = connect_zabbix(zabbix_server, username, password)

        # Сбор всех метрик
        all_metrics = []
        for host_name in host_names:
            host_metrics = collect_host_metrics(zapi, host_name, days_back, timezone_str)
            all_metrics.extend(host_metrics)

        # Создание DataFrame и сохранение в Excel
        if all_metrics:
            df = pd.DataFrame(all_metrics, columns=DATA_COLUMNS)

            # Создание директории, если не существует
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)

            df.to_excel(output_path, index=False)
        else:
            # Создаем пустой файл с заголовками
            df = pd.DataFrame(columns=DATA_COLUMNS)
            df.to_excel(output_path, index=False)

        # Возврат результата
        module.exit_json(
            changed=True,
            excel_file=output_path,
            metrics=all_metrics,
            msg=f"Successfully collected metrics for {len(host_names)} hosts"
        )

    except Exception as e:
        module.fail_json(msg=f"Error collecting metrics: {str(e)}")


if __name__ == '__main__':
    main()