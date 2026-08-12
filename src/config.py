"""Configuration management: YAML file + environment variable overrides."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:
    import yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)


@dataclass
class EmailConfig:
    user: str = ""
    password: str = ""
    to: str = ""
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587


@dataclass
class SlackConfig:
    webhook_url: str = ""


@dataclass
class DiscordConfig:
    webhook_url: str = ""


@dataclass
class DatabaseConfig:
    path: str = "state/jobs.db"


@dataclass
class FilterConfig:
    require_us_location: bool = True


@dataclass
class BoardsConfig:
    csv: str = ""
    batch_size: int = 50
    workers: int = 12
    timeout: int = 30


@dataclass
class SourceConfig:
    enabled: bool = True
    max_jobs: int = 300


@dataclass
class Config:
    email: EmailConfig = field(default_factory=EmailConfig)
    slack: SlackConfig = field(default_factory=SlackConfig)
    discord: DiscordConfig = field(default_factory=DiscordConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    filter: FilterConfig = field(default_factory=FilterConfig)
    boards: BoardsConfig = field(default_factory=BoardsConfig)
    http_timeout: int = 30
    sources: dict[str, SourceConfig] = field(default_factory=dict)

    def source(self, name: str) -> SourceConfig:
        return self.sources.get(name, SourceConfig())

    @classmethod
    def load(cls, path: Optional[str] = None) -> "Config":
        """Load config from YAML file, then apply environment variable overrides."""
        raw: dict = {}

        config_path = path or os.environ.get("CONFIG_PATH", "config.yaml")
        resolved = _resolve_path(config_path)
        if resolved and _HAS_YAML:
            with open(resolved, encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
        elif resolved and not _HAS_YAML:
            print(f"[WARN] PyYAML not installed; ignoring config file {resolved}")

        cfg = cls()

        # Email — env vars always win
        e = raw.get("email", {}) or {}
        cfg.email.user = os.environ.get("EMAIL_USER", e.get("user", ""))
        cfg.email.password = os.environ.get("EMAIL_APP_PASSWORD", e.get("password", ""))
        cfg.email.to = os.environ.get("ALERT_TO_EMAIL", e.get("to", ""))
        cfg.email.smtp_host = e.get("smtp_host", "smtp.gmail.com")
        cfg.email.smtp_port = int(e.get("smtp_port", 587))

        # Slack
        sl = raw.get("slack", {}) or {}
        cfg.slack.webhook_url = os.environ.get("SLACK_WEBHOOK_URL", sl.get("webhook_url", ""))

        # Discord
        dc = raw.get("discord", {}) or {}
        cfg.discord.webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", dc.get("webhook_url", ""))

        # Database
        db = raw.get("database", {}) or {}
        cfg.database.path = os.environ.get("DB_PATH", db.get("path", "state/jobs.db"))

        # Filter
        fi = raw.get("filter", {}) or {}
        cfg.filter.require_us_location = _bool(
            os.environ.get("REQUIRE_US_LOCATION", fi.get("require_us_location", True))
        )

        # HTTP timeout
        cfg.http_timeout = int(os.environ.get("HTTP_TIMEOUT", raw.get("http_timeout", 30)))

        # Boards
        bo = raw.get("boards", {}) or {}
        cfg.boards.csv = os.environ.get("BOARDS_CSV", bo.get("csv", ""))
        cfg.boards.batch_size = int(os.environ.get("BOARDS_BATCH_SIZE", bo.get("batch_size", 50)))
        cfg.boards.workers = int(os.environ.get("BOARDS_WORKERS", bo.get("workers", 12)))
        cfg.boards.timeout = int(os.environ.get("BOARDS_TIMEOUT", bo.get("timeout", 30)))

        # Per-source config
        src_raw = raw.get("sources", {}) or {}
        defaults = {
            "microsoft": 300, "nvidia": 300, "amazon": 300,
            "goldman_sachs": 200, "ibm": 200, "oracle": 200,
            "meta": 200, "google": 200, "apple": 200,
            "netflix": 200, "stripe": 200,
            "linkedin": 100,
        }
        for name, default_max in defaults.items():
            s = src_raw.get(name, {}) or {}
            cfg.sources[name] = SourceConfig(
                enabled=_bool(s.get("enabled", True)),
                max_jobs=int(os.environ.get(f"MAX_{name.upper()}_JOBS", s.get("max_jobs", default_max))),
            )

        return cfg


def _resolve_path(raw: str) -> Optional[str]:
    if not raw:
        return None
    p = Path(os.path.expanduser(raw))
    if p.is_absolute():
        return str(p) if p.exists() else None
    for base in (Path.cwd(), Path(ROOT_DIR)):
        candidate = base / p
        if candidate.exists():
            return str(candidate)
    return None


def _bool(val) -> bool:
    if isinstance(val, bool):
        return val
    return str(val).lower() in ("1", "true", "yes")
