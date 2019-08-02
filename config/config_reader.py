import os
import yaml

LOCAL_DIR = os.path.dirname(os.path.abspath(__file__))


def fetch_configuration():
    config_files = config_yaml_precedence()
    default_config = compile_all_config_yaml(config_files)
    environment = fetch_env_vars(default_config.keys())
    if environment:
        print("These settings are being overwritten by env vars: {}".format(", ".join(environment.keys())))
        default_config.update(environment)

    return default_config


def config_yaml_precedence():
    files = ["base.yaml"]
    if os.environ.get("APP_CONFIG_YAML"):
        files = os.environ["APP_CONFIG_YAML"].split(",")
    print("Using these files: {}".format(" -> ".join(files)))
    return files


def compile_all_config_yaml(file_list):
    final_file_config = {}
    config_dicts = [read_yaml(os.path.join(LOCAL_DIR, f)) for f in file_list]
    for d in config_dicts:
        final_file_config.update(d)
    return final_file_config


def read_yaml(file_path):
    return yaml.load(open(file_path), Loader=yaml.SafeLoader)


def fetch_env_vars(var_list: list) -> dict:
    """
        Returns a dictionary of any config keys if an enviroment variable with
        the same name exists with a truthy value.

        Example: a config variable of "cfda_file" will make this function search for
            an environment variable of "CFDA_FILE".
            If CFDA_FILE does not exist or is an empty string, it isn't returned by this function
            If it does have a legit value, include the key-value pair
    """
    env_vars = {
        config_key: os.environ.get(config_key.upper()) for config_key in var_list if os.environ.get(config_key.upper())
    }
    return handle_config_types(env_vars)


def handle_config_types(config_dict):
    new_config_dict = {}
    for k, v in config_dict.items():
        new_config_dict[k] = v
        if can_be_boolean(v):
            new_config_dict[k] = convert_to_boolean(v)
        elif str(v).isnumeric():
            new_config_dict[k] = int(v)
        else:
            try:
                new_config_dict[k] = float(v)
            except ValueError:
                pass

    return new_config_dict


def can_be_boolean(value):
    if str(value).upper() == "TRUE" or str(value).upper() == "FALSE":
        return True
    return False


def convert_to_boolean(value):
    if str(value).upper() == "TRUE":
        return True
    else:
        return False
