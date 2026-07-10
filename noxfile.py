"""nox configuration for qserv-kafka."""

import nox
from nox_uv import session

# Default sessions.
nox.options.sessions = ["lint", "typing", "test"]

# Other nox defaults.
nox.options.default_venv_backend = "uv"
nox.options.reuse_existing_virtualenvs = True


@session(name="coverage-report", uv_groups=["dev"])
def coverage_report(session: nox.Session) -> None:
    """Generate a code coverage report from the test suite."""
    session.run("coverage", "report", *session.posargs)


@session(uv_only_groups=["lint"], uv_no_install_project=True)
def lint(session: nox.Session) -> None:
    """Run pre-commit hooks."""
    session.run("pre-commit", "run", "--all-files", *session.posargs)


@session(uv_groups=["dev"])
def test(session: nox.Session) -> None:
    """Test the Semaphore server."""
    session.run(
        "pytest",
        "--cov=qservkafka",
        "--cov-branch",
        "--cov-report=",
        *session.posargs,
        env={
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
            "METRICS_APPLICATION": "qserv-kafka",
            "METRICS_ENABLED": "false",
            "METRICS_MOCK": "true",
            "QSERV_KAFKA_GAFAELFAWR_TOKEN": "fake-token",
            "QSERV_KAFKA_LOG_LEVEL": "DEBUG",
            "QSERV_KAFKA_REDIS_PASSWORD": "INSECURE-PASSWORD",
            "QSERV_KAFKA_REDIS_URL": "redis://localhost/0",
            "QSERV_KAFKA_QSERV_DATABASE_PASSWORD": "INSECURE-PASSWORD",
            "QSERV_KAFKA_QSERV_DATABASE_POOL_SIZE": "1",
            "QSERV_KAFKA_QSERV_DATABASE_URL": "mysql+asyncmy://localhost/qserv",
            "QSERV_KAFKA_QSERV_REST_URL": "https://qserv.example.com/",
            "QSERV_KAFKA_TAP_SERVICE": "qserv",
        },
    )


@session(uv_groups=["dev", "typing"])
def typing(session: nox.Session) -> None:
    """Run mypy."""
    session.run(
        "mypy",
        *session.posargs,
        "noxfile.py",
        "src",
        "tests",
    )
