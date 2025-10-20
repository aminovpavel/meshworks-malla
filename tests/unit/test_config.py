# New unit tests for configuration loader

from pathlib import Path

from malla.config import AppConfig, _clear_config_cache, load_config


def test_yaml_loading(tmp_path: Path, monkeypatch):
    """Ensure that values from a YAML file are loaded into AppConfig."""

    # Clear any cached config from other imports
    _clear_config_cache()

    # Clear any environment variables that might override the YAML
    monkeypatch.delenv("MALLA_NAME", raising=False)
    monkeypatch.delenv("MALLA_PORT", raising=False)
    monkeypatch.delenv("MALLA_HOME_MARKDOWN", raising=False)

    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("""
name: CustomName
home_markdown: "# Welcome\nThis is **markdown** content."
port: 9999
""")

    cfg = load_config(config_path=yaml_file)

    assert isinstance(cfg, AppConfig)
    assert cfg.name == "CustomName"
    assert "markdown" in cfg.home_markdown
    assert cfg.port == 9999


def test_env_override(monkeypatch):
    """Environment variables with the `MALLA_` prefix override YAML/defaults."""

    # Clear any cached config from other imports
    _clear_config_cache()

    monkeypatch.setenv("MALLA_NAME", "EnvName")
    monkeypatch.setenv("MALLA_DEBUG", "true")
    monkeypatch.setenv("MALLA_MESHPIPE_USE_GRPC", "1")
    monkeypatch.setenv("MALLA_MESHPIPE_GRPC_ENDPOINT", "meshpipe-proxy:8443")
    monkeypatch.setenv("MALLA_MESHPIPE_GRPC_TIMEOUT_SECONDS", "12.5")
    cfg = load_config(config_path=None)

    assert cfg.name == "EnvName"
    assert cfg.debug is True
    assert cfg.meshpipe_use_grpc is True
    assert cfg.meshpipe_grpc_endpoint == "meshpipe-proxy:8443"
    assert cfg.meshpipe_grpc_timeout_seconds == 12.5
