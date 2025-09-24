from yaml import safe_load


def load_config(path: str = "../config.yaml"):
    with open(path, "r") as f:
        config = safe_load(f)
    return config
