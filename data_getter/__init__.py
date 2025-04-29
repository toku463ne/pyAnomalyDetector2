def get_data_getter(data_source_config):
    if data_source_config['type'] == 'csv':
        from data_getter.csv_getter import CsvGetter
        return CsvGetter(data_source_config)
    if data_source_config['type'] == 'zabbix':
        from data_getter.zabbix_getter import ZabbixGetter
        return ZabbixGetter(data_source_config)
    if data_source_config['type'] == 'logan':
        from data_getter.logan_getter import LoganGetter
        return LoganGetter(data_source_config)
    