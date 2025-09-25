from yaml import safe_load


def load_config(path: str = "../config.yml"):
    with open(path, "r") as f:
        config = safe_load(f)
    return config
