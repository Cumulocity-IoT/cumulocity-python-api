import json as vanilla_json
from pathlib import Path

import pytest
import ujson
import orjson


@pytest.fixture(scope="module", name="mid_json")
def fix_mid_json():
    """Provide a medium-sized, standard Cumulocity JSON result as String."""
    file_path = Path(__file__).parent / "mo100.json"
    return file_path.read_text(encoding="utf-8")


@pytest.mark.benchmark
def test_vanilla(benchmark, mid_json):
    """Test the performance of vanilla JSON."""
    benchmark(vanilla_json.loads, mid_json)


@pytest.mark.benchmark
def test_ujson(benchmark, mid_json):
    """Test the performance of UltraJSON."""
    benchmark(ujson.loads, mid_json)


@pytest.mark.benchmark
def test_orjson(benchmark, mid_json):
    """Test the performance of OrJSON."""
    benchmark(orjson.loads, mid_json)
