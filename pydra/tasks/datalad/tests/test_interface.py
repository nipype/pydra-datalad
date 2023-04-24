import os
import typing as ty
from pathlib import Path
import datalad.api as dl
from ..interface.datalad import DataladInterface
from .utils import need_gitannex


@need_gitannex
def test_datalad_interface(tmpdir: Path):
    """
    Testing datalad interface
    """
    # Convert PosixPath to str and create a dataset
    tmpdir = str(tmpdir)
    ds = dl.Dataset(tmpdir).create()
    ds.save()
    ds_path = ds.pathobj

    # Create a file to download
    file_path = ds_path / "file.txt"
    file_path.write_text("test")
    ds.save()

    # Convert the tmpdir back to Path
    tmpdir = Path(tmpdir)

    # Install the dataset to a new location
    ds2 = dl.install(source=tmpdir, path=tmpdir / "ds2")
    ds2_path = ds2.pathobj

    # Use datalad interface to download the file
    dl_interface = DataladInterface(
        name="dl_interface", in_file="file.txt", dataset_path=ds2_path
    )

    # Run the task
    res = dl_interface()

    assert os.path.exists(res.output.out_file)
    assert os.path.basename(res.output.out_file) == "file.txt"