from repo.settings import CURRENT_SCHEMA_VERSION
from repo.settings.migrations import REGISTRY


def test_settings_schema_version_matches_migrations() -> None:
    assert CURRENT_SCHEMA_VERSION == max(REGISTRY, default=0) + 1
