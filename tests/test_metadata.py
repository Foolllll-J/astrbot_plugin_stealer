"""metadata.yaml 结构与最新 AstrBot 插件规范一致性测试。

参照文档：https://docs.astrbot.app/dev/star/plugin-new.html
（字段、PEP 440 版本约束、ADAPTER_NAME_2_TYPE 平台 key）
"""

from pathlib import Path

import pytest
import yaml
from packaging.specifiers import SpecifierSet

REPO_ROOT = Path(__file__).resolve().parents[1]
METADATA = REPO_ROOT / "metadata.yaml"

# AstrBot 文档列出的合法平台适配器 key（ADAPTER_NAME_2_TYPE）
ADAPTER_NAME_2_TYPE_KEYS = {
    "aiocqhttp",
    "qq_official",
    "qq_official_webhook",
    "telegram",
    "wecom",
    "wecom_ai_bot",
    "lark",
    "dingtalk",
    "discord",
    "slack",
    "kook",
    "vocechat",
    "weixin_official_account",
    "weixin_oc",
    "satori",
    "misskey",
    "line",
    "matrix",
    "mattermost",
}


def _load_metadata() -> dict:
    assert METADATA.exists(), "metadata.yaml 不存在"
    data = yaml.safe_load(METADATA.read_text(encoding="utf-8"))
    assert isinstance(data, dict), "metadata.yaml 必须是映射"
    return data


class TestRequiredFields:
    def test_required_fields_present_and_non_empty(self):
        data = _load_metadata()
        for field in ("name", "author", "desc", "version"):
            assert field in data, f"缺少必需字段 {field}"
            assert isinstance(data[field], str) and data[field].strip()

    def test_name_prefix_and_lowercase(self):
        data = _load_metadata()
        assert data["name"].startswith("astrbot_plugin_")
        assert data["name"] == data["name"].lower()
        assert " " not in data["name"]


class TestVersion:
    def test_version_has_no_v_prefix(self):
        # 最新规范：版本号不要加 v 前缀
        assert not str(_load_metadata()["version"]).startswith("v")

    def test_astrbot_version_is_pep440_without_v_prefix(self):
        raw = _load_metadata()["astrbot_version"]
        assert isinstance(raw, str) and raw.strip()
        assert not raw.startswith("v")
        SpecifierSet(raw)  # 非法 specifier 会抛出异常


class TestDisplayFields:
    def test_display_name_present(self):
        assert str(_load_metadata()["display_name"]).strip()

    def test_short_desc_present_and_shorter_than_desc(self):
        data = _load_metadata()
        assert "short_desc" in data, "最新文档要求提供 short_desc（市场卡片短描述）"
        short_desc = str(data["short_desc"]).strip()
        desc = str(data["desc"]).strip()
        assert short_desc
        assert len(short_desc) < len(desc)


class TestPlatforms:
    def test_support_platforms_are_valid_adapter_keys(self):
        platforms = _load_metadata().get("support_platforms")
        assert isinstance(platforms, list) and platforms
        for platform in platforms:
            assert platform in ADAPTER_NAME_2_TYPE_KEYS, f"未知平台适配器 key: {platform}"