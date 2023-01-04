# Tasks for testing
import subprocess as sp
import pytest
import shutil

need_gitannex = pytest.mark.skipif(
    not (shutil.which("git-annex"))
    or bool(
        float(
            sp.check_output(["git-annex", "version", "--raw"], universal_newlines=True)[
                :6
            ]
        )
        < 8.20200309
    ),
    reason="git-annex is not installed or version is less than 8.20200309",
)