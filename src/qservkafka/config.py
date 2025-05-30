"""Application settings."""

from __future__ import annotations

from datetime import timedelta

from arq.connections import RedisSettings
from pydantic import Field, HttpUrl, MySQLDsn, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from safir.arq import ArqMode, build_arq_redis_settings
from safir.kafka import KafkaConnectionSettings
from safir.logging import LogLevel, Profile
from safir.metrics import MetricsConfiguration, metrics_configuration_factory
from safir.pydantic import EnvRedisDsn, HumanTimedelta

__all__ = ["Config", "config"]


class Config(BaseSettings):
    """Configuration for qserv-kafka."""

    model_config = SettingsConfigDict(
        env_prefix="QSERV_KAFKA_", case_sensitive=False
    )

    arq_mode: ArqMode = Field(
        ArqMode.production,
        title="arq queue mode",
        description="Used by the test suite to switch to a mock queue",
    )

    consumer_group_id: str | None = Field(
        "qserv", title="Kafka consumer group ID"
    )

    kafka: KafkaConnectionSettings = Field(
        default_factory=KafkaConnectionSettings,
        title="Kafka connection settings",
    )

    job_cancel_topic: str = Field(
        "lsst.tap.job-delete", title="Topic for job cancellation requests"
    )

    job_run_topic: str = Field(
        "lsst.tap.job-run", title="Topic for job requests"
    )

    job_status_topic: str = Field(
        "lsst.tap.job-status", title="Topic for job status"
    )

    log_level: LogLevel = Field(
        LogLevel.INFO, title="Log level of the application's logger"
    )

    max_worker_jobs: int = Field(
        5, title="Simultaneous result worker jobs per pod"
    )

    metrics: MetricsConfiguration = Field(
        default_factory=metrics_configuration_factory,
        title="Metrics configuration",
    )

    name: str = Field("qserv-kafka", title="Name of application")

    profile: Profile = Field(
        Profile.production, title="Application logging profile"
    )

    redis_password: SecretStr = Field(
        ..., title="Redis password", description="Password for Redis server"
    )

    redis_url: EnvRedisDsn = Field(
        ...,
        title="Redis DSN",
        description="DSN for the Redis server storing query state",
    )

    qserv_database_password: SecretStr | None = Field(
        None, title="Qserv MySQL password"
    )

    qserv_database_overflow: int = Field(
        100,
        title="Qserv MySQL connection overflow",
        description=(
            "The maximum number of connections to open to the Qserv MySQL"
            " server above and beyond the pool size. Connections will then"
            " be closed when idle until the connection count returns to the"
            " pool size."
        ),
    )

    qserv_database_pool_size: int = Field(
        20,
        title="Qserv MySQL pool size",
        description=(
            "Number of Qserv MySQL connections to keep open. The number of"
            " open connections will not drop below this even if the Qserv"
            " Kafka bridge is idle."
        ),
    )

    qserv_database_url: MySQLDsn = Field(..., title="Qserv MySQL DSN")

    qserv_poll_interval: HumanTimedelta = Field(
        timedelta(seconds=1),
        title="Qserv poll interval",
        description="How frequently to poll Qserv for query status",
    )

    qserv_rest_max_connections: int = Field(
        20,
        title="Max Qserv REST API connections",
        description=(
            "How many connections the bridge will open to the Qserv REST API"
            " simultaneously"
        ),
    )

    qserv_rest_timeout: HumanTimedelta = Field(
        timedelta(seconds=30),
        title="Qserv REST timeout",
        description=(
            "Maximum timeout for a REST API call to Qserv. This includes the"
            " time spent waiting for a free connection."
        ),
    )

    qserv_rest_url: HttpUrl = Field(..., title="Qserv REST API URL")

    qserv_upload_timeout: HumanTimedelta = Field(
        timedelta(minutes=5),
        title="Qserv table upload timeout",
        description=(
            "Maximum timeout for a REST API call to Qserv to upload a table."
            " This includes the time spent waiting for a free connection."
        ),
    )

    rewrite_base_url: HttpUrl = Field(
        ...,
        title="Base URL for rewrites",
        description=(
            "URLs in columns flagged for rewriting will have their netloc"
            " portion replaced with the netloc from this base URL. The rest"
            " of the URL will be left untouched."
        ),
    )

    result_timeout: HumanTimedelta = Field(
        timedelta(minutes=10),
        title="Timeout for result processing",
        description=(
            "How long to wait for result processing: retrieving the result"
            " rows from Qserv, encoding them, and writing them to the upload"
            " PUT URL. Qserv deletes results once retrieved, so aborting"
            " result processing risks losing a result. This should be aligned"
            " with the Kubernetes shutdown grace period."
        ),
    )

    @property
    def arq_redis_settings(self) -> RedisSettings:
        """Redis settings for arq."""
        return build_arq_redis_settings(self.redis_url, self.redis_password)

    @field_validator("qserv_database_url")
    @classmethod
    def _validate_qserv_database_url(cls, v: MySQLDsn) -> MySQLDsn:
        """Ensure that the Qserv DSN uses a compatible dialect."""
        if v.scheme not in ("mysql", "mysql+asyncmy"):
            msg = "Only mysql or mysql+asyncmy DSN schemes are supported"
            raise ValueError(msg)
        return v


config = Config()
"""Configuration for qserv-kafka."""
