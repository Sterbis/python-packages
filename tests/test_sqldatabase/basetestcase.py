import json
import shutil
import unittest
from pathlib import Path
from typing import Any


class BaseTestCase(unittest.TestCase):
    data_dir_path = Path(__file__).parent / "data"
    test_data: dict[str, Any] = {}

    @property
    def test_name(self) -> str:
        return self._testMethodName

    @classmethod
    def setUpClass(cls):
        cls.get_temp_dir_path().mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.get_temp_dir_path().is_dir():
            shutil.rmtree(cls.get_temp_dir_path())
            # pass

    @classmethod
    def get_temp_dir_path(cls) -> Path:
        return Path(__file__).parent / "temp" / cls.__name__

    @classmethod
    def load_test_data(cls, file_name: str | None = None) -> None:
        file_name = file_name or f"{cls.__name__}.json"
        cls.test_data = cls.load_json_data(file_name)

    @classmethod
    def load_json_data(cls, file_name: str) -> Any:
        file_path = cls.data_dir_path / file_name
        with file_path.open("r", encoding="utf-8") as file:
            data = json.load(file)
        return data
