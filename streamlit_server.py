
import views.streamlit_view as v
import utils.config_loader as config_loader

if __name__ == "__main__":
    # read arguments
    import argparse
    parser = argparse.ArgumentParser(description='Starts the streamlit server.')
    parser.add_argument('-c', '--config', type=str, help='config yaml file')

    args = parser.parse_args()
    config_file = args.config
    config = config_loader.load_config(config_file)

    # start the streamlit server
    v.run(config)
