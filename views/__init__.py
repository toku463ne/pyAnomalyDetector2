def get_view(view_config):
    if view_config['type'] == 'flask':
        from views.flask_view import FlaskView
    elif view_config['type'] == 'zabbix_dashboard':
        from views.zabbix_dashboard import ZabbixDashboard
        return ZabbixDashboard(view_config)