from typing import Any, Union
import json
from pathlib import Path


def read_json(path: Union[Path, str]) -> Any:
    with open(path, "r") as f:
        json_obj = json.load(f)
    return json_obj
