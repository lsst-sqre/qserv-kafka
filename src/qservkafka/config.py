"""Application settings."""

from datetime import timedelta
from typing import Any, Self

from arq.connections import RedisSettings
from arq.constants import default_queue_name
from httpx import USE_CLIENT_DEFAULT, BasicAuth
from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    MySQLDsn,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from safir.arq import ArqMode, build_arq_redis_settings
from safir.kafka import KafkaConnectionSettings
from safir.logging import LogLevel, Profile
from safir.metrics import MetricsConfiguration, metrics_configuration_factory
from safir.pydantic import EnvRedisDsn, HumanTimedelta

__all__ = ["Config", "config"]


class SlackConfig(BaseSettings):
    """Configuration for Slack exception notifications."""

    enabled: bool = Field(
        False,
        title="Slack enabled",
        description="Whether to send exceptions to Slack",
        validation_alias="QSERV_KAFKA_SLACK_ENABLED",
    )

    webhook: SecretStr | None = Field(
        None,
        title="Slack webhook for alerts",
        description="Alerts will be posted to this Slack webhook",
        validation_alias="QSERV_KAFKA_SLACK_WEBHOOK",
    )

    @model_validator(mode="after")
    def validate_config(self) -> Self:
        if self.enabled and self.webhook is None:
            msg = "If enabled is true, then webhook must be set"
            raise ValueError(msg)
        return self


class SentryConfig(BaseModel):
    """Sentry configuration for qserv-kafka.

    This configuration is not used internally because we want to initialize
    Sentry directly from environment variables before the config initialized.
    It has to be present in the Config model because we want to forbid unknown
    configuration settings, but we want to specify the values for these env
    vars under the ``config`` key in Phalanx. If we don't declare these
    settings in the Config model, then we'd get an error when we try to
    instantiate it.
    """

    enabled: bool = Field(False, title="Whether to send exceptions to Sentry")
    traces_sample_rate: float = Field(
        0.0,
        title="Sentry trace sample rate",
        description="The percentage of traces to be sent to sentry.",
        ge=0.0,
        le=1.0,
    )


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

    arq_queue: str = Field(
        default_queue_name,
        title="arq queue",
        description="The arq queue name that worker will listen on",
    )

    consumer_group_id: str | None = Field(
        "qserv", title="Kafka consumer group ID"
    )

    gafaelfawr_token: SecretStr = Field(
        ...,
        title="Gafaelfawr token",
        description="Token to use for Gafaelfawr API calls",
    )

    kafka: KafkaConnectionSettings = Field(
        default_factory=KafkaConnectionSettings,
        title="Kafka connection settings",
    )

    job_cancel_topic: str = Field(
        "lsst.tap.job-delete", title="Topic for job cancellation requests"
    )

    job_run_batch_size: int = Field(10, title="Batch size for job requests")

    job_run_max_bytes: int = Field(
        10 * 1024 * 1024,
        title="Batch size in bytes for job requests",
        description=(
            "Wide queries may result in Kafka messages that are 500KiB or"
            " larger, so this number should be at least the batch size"
            " times the maximum typical size of a query request"
        ),
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

    log_profile: Profile = Field(
        Profile.production, title="Application logging profile"
    )

    max_worker_jobs: int = Field(
        2,
        title="Concurrent result worker jobs per pod",
        description=(
            "Result workers can only use one CPU and are CPU-bound, so setting"
            " this value higher than 2 will normally not be helpful"
        ),
    )

    metrics: MetricsConfiguration = Field(
        default_factory=metrics_configuration_factory,
        title="Metrics configuration",
    )

    sentry: SentryConfig | None = Field(None, title="Sentry configuration")

    slack: SlackConfig = Field(SlackConfig(), title="Slack configuration")

    name: str = Field("qserv-kafka", title="Name of application")

    parquet_batch_size: int = Field(
        2000,
        title="VOParquet encoding batch size",
        description=(
            "Number of rows to batch together when encoding Parquet files."
        ),
    )

    redis_max_connections: int = Field(
        15,
        title="Redis connection pool size",
        description=(
            "Should be larger than the batch size to handle simultaneous"
            " quota requests for the batch, plus an extra connection for"
            " the background monitor and another for cancel messages"
        ),
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
        20,
        title="Qserv MySQL connection overflow",
        description=(
            "The maximum number of connections to open to the Qserv MySQL"
            " server above and beyond the pool size. Connections will then"
            " be closed when idle until the connection count returns to the"
            " pool size."
        ),
    )

    qserv_database_pool_size: int = Field(
        10,
        title="Qserv MySQL pool size",
        description=(
            "Number of Qserv MySQL connections to keep open. The number of"
            " open connections will not drop below this even if the Qserv"
            " Kafka bridge is idle."
        ),
    )

    qserv_database_connect_timeout: int = Field(
        10,
        title="Qserv database connection timeout",
        description=(
            "Timeout (seconds) for establishing a database connection to Qserv"
        ),
    )

    qserv_database_read_timeout: int = Field(
        60,
        title="Qserv database read timeout",
        description="Timeout (seconds) for reading from the Qserv database",
    )

    qserv_database_url: MySQLDsn = Field(..., title="Qserv MySQL DSN")

    qserv_delete_queries: bool = Field(
        True,
        title="Delete completed queries",
        description=(
            "Whether to clean up completed queries explicitly after each"
            " query completes. If set to false, the query results are left"
            " in Qserv for it to clean up itself."
        ),
    )

    qserv_poll_interval: HumanTimedelta = Field(
        timedelta(seconds=1),
        title="Qserv poll interval",
        description="How frequently to poll Qserv for query status",
    )

    qserv_rest_max_connections: int = Field(
        15,
        title="Max Qserv REST API connections",
        description=(
            "How many connections the bridge will open to the Qserv REST API"
            " simultaneously. This should be set to a little larger than the"
            " job run queue batch size to provide some headroom for the"
            " job monitor and any cancel processing."
        ),
    )

    qserv_rest_password: SecretStr | None = Field(
        None,
        title="Qserv REST API password",
        description=(
            "HTTP Basic Authentication password. If set, the REST API username"
            " must also be set."
        ),
    )

    qserv_rest_send_api_version: bool = Field(
        True,
        title="Send Qserv REST API version",
        description=(
            "If true, send the expected Qserv REST API version as part of"
            " each request, which will cause Qserv to reject the request if"
            " the API version is not known"
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

    qserv_rest_username: str | None = Field(
        None,
        title="Qserv REST API username",
        description=(
            "HTTP Basic Authentication username. If set, the REST API username"
            " must also be set."
        ),
    )

    qserv_retry_count: int = Field(
        3,
        title="Number of Qserv retries",
        description=(
            "How many times to retry a Qserv API call before giving up"
        ),
    )

    qserv_retry_delay: HumanTimedelta = Field(
        timedelta(seconds=1),
        title="Delay between Qserv retries",
        description="How long to pause between retries of Qserv API calls",
    )

    qserv_upload_timeout: HumanTimedelta = Field(
        timedelta(minutes=5),
        title="Qserv table upload timeout",
        description=(
            "Maximum timeout for a REST API call to Qserv to upload a table."
            " This includes the time spent waiting for a free connection."
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

    tap_service: str = Field(
        ...,
        title="TAP service name",
        description="Used to determine which TAP quota to apply",
    )

    @property
    def arq_redis_settings(self) -> RedisSettings:
        """Redis settings for arq."""
        return build_arq_redis_settings(self.redis_url, self.redis_password)

    @property
    def rest_authentication(self) -> Any:
        """Authentication setting for Qserv REST API client."""
        if self.qserv_rest_username and self.qserv_rest_password:
            return BasicAuth(
                username=self.qserv_rest_username,
                password=self.qserv_rest_password.get_secret_value(),
            )
        else:
            return USE_CLIENT_DEFAULT

    @field_validator("qserv_database_url")
    @classmethod
    def _validate_qserv_database_url(cls, v: MySQLDsn) -> MySQLDsn:
        """Ensure that the Qserv DSN uses a compatible dialect."""
        if v.scheme not in ("mysql", "mysql+asyncmy"):
            msg = "Only mysql or mysql+asyncmy DSN schemes are supported"
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def _validate_qserv_rest_authentication(self) -> Self:
        have_username = self.qserv_rest_username is not None
        have_password = self.qserv_rest_password is not None
        if have_username != have_password:
            msg = "Set both or neither of REST API username and password"
            raise ValueError(msg)
        return self


config = Config()
"""Configuration for qserv-kafka."""
