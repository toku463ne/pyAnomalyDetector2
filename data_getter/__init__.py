def get_data_getter(data_source_config):
    if data_source_config['type'] == 'csv':
        from data_getter.csv_getter import CsvGetter
        return CsvGetter(data_source_config)