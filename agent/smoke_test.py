"""Fixture-based smoke tests for DaYu agent layer."""

import json
import os
import sys
import tempfile

import pytest

AGENT_DIR = os.path.dirname(os.path.abspath(__file__))
DAYU_ROOT = os.path.dirname(AGENT_DIR)
FIXTURE_DIR = os.path.join(DAYU_ROOT, "flow_analysis", "example_stat", "ddmd")

sys.path.insert(0, AGENT_DIR)


class TestFixturePresence:
    def test_has_vol_stat_files(self):
        vol_files = [f for f in os.listdir(FIXTURE_DIR) if f.endswith("-vol_data_stat.json")]
        assert len(vol_files) >= 1

    def test_has_vfd_stat_files(self):
        vfd_files = [f for f in os.listdir(FIXTURE_DIR) if f.endswith("-vfd_data_stat.json")]
        assert len(vfd_files) >= 1

    def test_has_task_order_list(self):
        assert os.path.isfile(os.path.join(FIXTURE_DIR, "task_order_list.json"))

    def test_has_task_to_file_map(self):
        maps = [f for f in os.listdir(FIXTURE_DIR) if f.endswith("-task_to_file.json")]
        assert len(maps) >= 1


class TestStatFileSchema:
    def test_vol_stat_structure(self):
        vol_files = [f for f in os.listdir(FIXTURE_DIR) if f.endswith("-vol_data_stat.json")]
        path = os.path.join(FIXTURE_DIR, vol_files[0])
        with open(path) as f:
            data = json.load(f)
        assert isinstance(data, list)
        first = data[0]
        key = list(first.keys())[0]
        assert "file_name" in first[key]
        assert "task_name" in first[key]
        assert "datasets" in first[key]

    def test_vfd_stat_structure(self):
        vfd_files = [f for f in os.listdir(FIXTURE_DIR) if f.endswith("-vfd_data_stat.json")]
        path = os.path.join(FIXTURE_DIR, vfd_files[0])
        with open(path) as f:
            data = json.load(f)
        assert isinstance(data, list)
        first = data[0]
        key = list(first.keys())[0]
        assert "file_name" in first[key]
        assert "task_name" in first[key]
        assert "access_type" in first[key]


class TestVolOnlyAnalysis:
    def test_build_vol_sankey(self):
        from analysis.vol_only import build_vol_sankey
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            output_html = f.name
        try:
            result = build_vol_sankey(FIXTURE_DIR, output_html, test_name="ddmd")
            assert isinstance(result, dict)
            assert "nodes" in result
            assert "links" in result
            assert "meta" in result
            assert len(result["nodes"]) > 0
            assert len(result["links"]) > 0
            assert os.path.isfile(output_html)
            assert os.path.getsize(output_html) > 1000
        finally:
            if os.path.exists(output_html):
                os.unlink(output_html)


class TestVfdOnlyAnalysis:
    def test_build_vfd_sankey(self):
        from analysis.vfd_only import build_vfd_sankey
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            output_html = f.name
        try:
            result = build_vfd_sankey(FIXTURE_DIR, output_html, test_name="ddmd")
            assert isinstance(result, dict)
            assert "nodes" in result
            assert "links" in result
            assert "meta" in result
            assert len(result["nodes"]) > 0
            assert len(result["links"]) > 0
            assert os.path.isfile(output_html)
            assert os.path.getsize(output_html) > 1000
        finally:
            if os.path.exists(output_html):
                os.unlink(output_html)


class TestCombinedAnalysis:
    def test_build_combined_sankey(self):
        from analysis.vol_vfd_combined import build_combined_sankey
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            output_html = f.name
        try:
            result = build_combined_sankey(FIXTURE_DIR, output_html, test_name="ddmd")
            assert isinstance(result, dict)
            assert "nodes" in result
            assert "links" in result
            assert "meta" in result
            assert len(result["nodes"]) > 0
            assert len(result["links"]) > 0
            assert os.path.isfile(output_html)
            assert os.path.getsize(output_html) > 1000
        finally:
            if os.path.exists(output_html):
                os.unlink(output_html)
