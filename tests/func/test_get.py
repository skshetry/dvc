import logging
import os

import pytest

from dvc.cache import Cache
from dvc.exceptions import PathMissingError
from dvc.main import main
from dvc.repo import Repo
from dvc.repo.get import GetDVCFileError
from dvc.system import System
from dvc.utils.fs import makedirs


def test_get_repo_file(tmp_dir, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file", "contents", commit="create file")

    Repo.get(os.fspath(erepo_dir), "file", "file_imported")

    assert os.path.isfile("file_imported")
    assert (tmp_dir / "file_imported").read_text() == "contents"


def test_get_repo_dir(tmp_dir, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen({"dir": {"file": "contents"}}, commit="create dir")

    Repo.get(os.fspath(erepo_dir), "dir", "dir_imported")

    assert (tmp_dir / "dir_imported").read_text() == {"file": "contents"}


@pytest.mark.parametrize(
    "erepo", [pytest.lazy_fixture("git_dir"), pytest.lazy_fixture("erepo_dir")]
)
def test_get_git_file(tmp_dir, erepo):
    src = "some_file"
    dst = "some_file_imported"

    erepo.scm_gen({src: "hello"}, commit="add a regular file")

    Repo.get(os.fspath(erepo), src, dst)

    assert (tmp_dir / dst).read_text() == "hello"


@pytest.mark.parametrize(
    "erepo", [pytest.lazy_fixture("git_dir"), pytest.lazy_fixture("erepo_dir")]
)
def test_get_git_dir(tmp_dir, erepo):
    src = "some_directory"
    dst = "some_directory_imported"

    erepo.scm_gen(
        {src: {"dir": {"file.txt": "hello"}}}, commit="add a regular dir"
    )

    Repo.get(os.fspath(erepo), src, dst)

    assert (tmp_dir / dst).read_text() == {"dir": {"file.txt": "hello"}}


def test_cache_type_is_properly_overridden(tmp_dir, erepo_dir):
    with erepo_dir.chdir():
        with erepo_dir.dvc.config.edit() as conf:
            conf["cache"]["type"] = "symlink"
        erepo_dir.dvc.cache = Cache(erepo_dir.dvc)
        erepo_dir.scm_add(
            [erepo_dir.dvc.config.files["repo"]], "set cache type to symlinks"
        )
        erepo_dir.dvc_gen("file", "contents", "create file")
    assert System.is_symlink(erepo_dir / "file")

    Repo.get(os.fspath(erepo_dir), "file", "file_imported")

    assert not System.is_symlink("file_imported")
    assert (tmp_dir / "file_imported").read_text() == "contents"


def test_get_repo_rev(tmp_dir, erepo_dir):
    with erepo_dir.chdir(), erepo_dir.branch("branch", new=True):
        erepo_dir.dvc_gen("file", "contents", commit="create file on branch")

    Repo.get(os.fspath(erepo_dir), "file", "file_imported", rev="branch")
    assert (tmp_dir / "file_imported").read_text() == "contents"


def test_get_from_non_dvc_repo(tmp_dir, git_dir):
    git_dir.scm_gen({"some_file": "contents"}, commit="create file")

    Repo.get(os.fspath(git_dir), "some_file", "file_imported")
    assert (tmp_dir / "file_imported").read_text() == "contents"


def test_get_a_dvc_file(tmp_dir, erepo_dir):
    with pytest.raises(GetDVCFileError):
        Repo.get(os.fspath(erepo_dir), "some_file.dvc")


def test_non_cached_output(tmp_dir, erepo_dir):
    src = "non_cached_file"
    dst = src + "_imported"

    with erepo_dir.chdir():
        erepo_dir.dvc.run(
            outs_no_cache=[src],
            cmd="echo hello > non_cached_file",
            single_stage=True,
        )
        erepo_dir.scm_add([src, src + ".dvc"], commit="add non-cached output")

    Repo.get(os.fspath(erepo_dir), src, dst)

    assert (tmp_dir / dst).is_file()
    # NOTE: using strip() to account for `echo` differences on win and *nix
    assert (tmp_dir / dst).read_text().strip() == "hello"


# https://github.com/iterative/dvc/pull/2837#discussion_r352123053
def test_absolute_file_outside_repo(tmp_dir, erepo_dir):
    with pytest.raises(PathMissingError):
        Repo.get(os.fspath(erepo_dir), "/root/")


def test_absolute_file_outside_git_repo(tmp_dir, git_dir):
    with pytest.raises(PathMissingError):
        Repo.get(os.fspath(git_dir), "/root/")


def test_unknown_path(tmp_dir, erepo_dir):
    with pytest.raises(PathMissingError):
        Repo.get(os.fspath(erepo_dir), "a_non_existing_file")


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_get_to_dir(tmp_dir, erepo_dir, dname):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file", "contents", commit="create file")

    makedirs(dname, exist_ok=True)

    Repo.get(os.fspath(erepo_dir), "file", dname)

    assert (tmp_dir / dname).is_dir()
    assert (tmp_dir / dname / "file").read_text() == "contents"


def test_get_from_non_dvc_master(tmp_dir, git_dir):
    with git_dir.chdir(), git_dir.branch("branch", new=True):
        git_dir.init(dvc=True)
        git_dir.dvc_gen("some_file", "some text", commit="create some file")

    Repo.get(os.fspath(git_dir), "some_file", out="some_dst", rev="branch")

    assert (tmp_dir / "some_dst").read_text() == "some text"


def test_get_file_from_dir(tmp_dir, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen(
            {
                "dir": {
                    "1": "1",
                    "2": "2",
                    "subdir": {"foo": "foo", "bar": "bar"},
                }
            },
            commit="create dir",
        )

    Repo.get(os.fspath(erepo_dir), os.path.join("dir", "1"))
    assert (tmp_dir / "1").read_text() == "1"

    Repo.get(os.fspath(erepo_dir), os.path.join("dir", "2"), out="file")
    assert (tmp_dir / "file").read_text() == "2"

    Repo.get(os.fspath(erepo_dir), os.path.join("dir", "subdir"))
    assert (tmp_dir / "subdir" / "foo").read_text() == "foo"
    assert (tmp_dir / "subdir" / "bar").read_text() == "bar"

    Repo.get(
        os.fspath(erepo_dir), os.path.join("dir", "subdir", "foo"), out="X"
    )
    assert (tmp_dir / "X").read_text() == "foo"


def test_get_url_positive(tmp_dir, erepo_dir, caplog, local_cloud):
    erepo_dir.add_remote(config=local_cloud.config)
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo")
    erepo_dir.dvc.push()

    caplog.clear()
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert main(["get", os.fspath(erepo_dir), "foo", "--show-url"]) == 0
        assert caplog.text == ""


def test_get_url_not_existing(tmp_dir, erepo_dir, caplog):
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert (
            main(
                [
                    "get",
                    os.fspath(erepo_dir),
                    "not-existing-file",
                    "--show-url",
                ]
            )
            == 1
        )
        assert "failed to show URL" in caplog.text


def test_get_url_git_only_repo(tmp_dir, scm, caplog):
    tmp_dir.scm_gen({"foo": "foo"}, commit="initial")

    with caplog.at_level(logging.ERROR):
        assert main(["get", os.fspath(tmp_dir), "foo", "--show-url"]) == 1
        assert "failed to show URL" in caplog.text


def test_get_pipeline_tracked_outs(
    tmp_dir, dvc, scm, git_dir, run_copy, local_remote
):
    from dvc.dvcfile import PIPELINE_FILE, PIPELINE_LOCK

    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    dvc.push()

    dvc.scm.add([PIPELINE_FILE, PIPELINE_LOCK])
    dvc.scm.commit("add pipeline stage")

    with git_dir.chdir():
        Repo.get("file:///{}".format(os.fspath(tmp_dir)), "bar", out="baz")
        assert (git_dir / "baz").read_text() == "foo"


def make_subrepo(dir_, scm, config):
    dir_.mkdir(parents=True)
    with dir_.chdir():
        dir_.scm = scm
        dir_.init(dvc=True, subdir=True)
        dir_.add_remote(config=config)


@pytest.mark.parametrize(
    "output",
    [
        "foo",
        {"foo": "foo", "bar": "bar"},
        {"subdir": {"foo": "foo", "bar": "bar"}},
    ],
    ids=["file", "dir", "nested_dir"],
)
@pytest.mark.parametrize(
    "erepo", [pytest.lazy_fixture("erepo_dir"), pytest.lazy_fixture("git_dir")]
)
@pytest.mark.parametrize(
    "subrepo_paths",
    [
        (os.path.join("sub", "subdir1"), os.path.join("sub", "subdir2")),
        (os.path.join("sub"), os.path.join("sub", "subdir1")),
    ],
    ids=["isolated", "nested"],
)
def test_subrepo_multiple(
    tmp_dir, scm, output, subrepo_paths, erepo, local_cloud
):
    sub_repos = [erepo / path for path in subrepo_paths]
    filename = "output"
    for repo in sub_repos:
        make_subrepo(repo, erepo.scm, local_cloud.config)
        repo.dvc_gen({filename: output}, commit="add subrepo")
        repo.dvc.push()

    for i, repo in enumerate(sub_repos):
        Repo.get(
            f"file:///{erepo}",
            str((repo / filename).relative_to(erepo)),
            out=f"{filename}-{i}",
        )
        assert (tmp_dir / f"{filename}-{i}").read_text() == output
