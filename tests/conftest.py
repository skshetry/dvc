import os

import pytest

from .dir_helpers import *  # noqa, pylint: disable=wildcard-import
from .remotes import *  # noqa, pylint: disable=wildcard-import

# Prevent updater and analytics from running their processes
os.environ["DVC_TEST"] = "true"
# Ensure progress output even when not outputting to raw sys.stderr console
os.environ["DVC_IGNORE_ISATTY"] = "true"

REMOTES = {
    # remote: enabled_by_default?
    "azure": False,
    "gdrive": False,
    "gs": False,
    "hdfs": False,
    "http": True,
    "oss": False,
    "s3": True,
    "ssh": True,
    "webdav": True,
}


@pytest.fixture(autouse=True)
def reset_loglevel(request, caplog):
    """
    Use it to ensure log level at the start of each test
    regardless of dvc.logger.setup(), Repo configs or whatever.
    """
    level = request.config.getoption("--log-level")
    if level:
        with caplog.at_level(level.upper(), logger="dvc"):
            yield
    else:
        yield


@pytest.fixture(scope="session", autouse=True)
def _close_pools():
    from dvc.tree.pool import close_pools

    yield
    close_pools()


def _get_opt_for_remote(name):
    action = "disable" if REMOTES[name] else "enable"
    return f"--{action}-{name}", action


def pytest_addoption(parser):
    """Adds remote-related flags to selectively disable/enable for tests
    Eg: If some remotes, eg: ssh is enabled to be tested for by default
    (see above `REMOTES`), then, `--disable-ssh` flag is added. If remotes
    like `hdfs` are disabled by default, `--enable-hdfs` is added to make them
    run.

    You can also make everything run-by-default with `--all` flag, which takes
    precedence on all previous `--enable-*`/`--disable-*` flags.
    """
    parser.addoption(
        "--all",
        action="store_true",
        default=False,
        help="Test all of the remotes",
    )
    for remote_name in REMOTES:
        opt, action = _get_opt_for_remote(remote_name)
        parser.addoption(
            opt,
            action="store_true",
            default=False,
            help=f"{action} tests for {remote_name}",
        )


class DVCTestConfig:
    def __init__(self):
        self.enabled_remotes = set()

    def requires(self, remote_name):
        if remote_name not in REMOTES or remote_name in self.enabled_remotes:
            return

        opt, _ = _get_opt_for_remote(remote_name)
        hint = "{} {}".format("Remove" if REMOTES[remote_name] else "Use", opt)
        pytest.skip(
            f"{remote_name} tests not enabled. {hint} to make this run"
        )

    def apply_marker(self, marker):
        self.requires(marker.name)


def pytest_runtest_setup(item):
    # Apply test markers to skip tests selectively
    # NOTE: this only works on individual tests,
    # for fixture, use `test_config` fixture and
    # run `test_config.requires(remote_name)`.
    for marker in item.iter_markers():
        item.config.dvc_config.apply_marker(marker)


@pytest.fixture(scope="session")
def test_config(request):
    return request.config.dvc_config


def pytest_configure(config):
    config.dvc_config = DVCTestConfig()

    for remote_name in REMOTES:
        config.addinivalue_line(
            "markers", f"{remote_name}: mark test as requiring {remote_name}"
        )

    enabled_remotes = config.dvc_config.enabled_remotes
    if config.getoption("--all"):
        enabled_remotes.update(REMOTES)
        return

    for remote_name, default in REMOTES.items():
        opt, _ = _get_opt_for_remote(remote_name)
        arg = config.getoption(opt)
        if (default and not arg) or (not default and arg):
            enabled_remotes.add(remote_name)
