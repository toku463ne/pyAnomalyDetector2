def get_view(view_config):
    if view_config['type'] == 'flask':
        from views.flask_view import FlaskView
        