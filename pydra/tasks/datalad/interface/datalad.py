import os
import typing as ty
from pathlib import Path
from pydra.engine.specs import (
    File,
    Directory,
    SpecInfo,
    BaseSpec,
)
from pydra.engine.core import TaskBase
from pydra.engine.helpers import output_from_inputfields
from pydra.utils.messenger import AuditFlag

input_fields = [
    ("in_file", str, {"help_string": "Path to the data to be downloaded through datalad", "mandatory": True}),
    ("dataset_path", Directory, {"help_string": "Path to the dataset that will be used to get data", "mandatory": True}),
    ("dataset_url", str, {"help_string": "URL to the dataset that will be used to get data"}),
]

output_fields = [
    ("out_file", File, {"help_string": "file downloaded through datalad", "requires": ["in_file"], "output_file_template": "{in_file}"}),
]

class DataladInterface(TaskBase):
    """A :obj:`~nipype.interfaces.utility.base.IdentityInterface` with a grafted Datalad getter."""

    def __init__(
        self,
        name: str,
        audit_flags: AuditFlag = AuditFlag.NONE,
        cache_dir=None,
        cache_locations=None,
        input_spec: ty.Optional[ty.Union[SpecInfo, BaseSpec]] = None,
        output_spec: ty.Optional[ty.Union[SpecInfo, BaseSpec]] = None,
        cont_dim=None,
        messenger_args=None,
        messengers=None,
        rerun=False,
        **kwargs,
    ):
        """Initialize a DataladInterface instance."""

        self.input_spec = input_spec or SpecInfo(name="Inputs", fields=input_fields, bases=(BaseSpec,))
        self.output_spec = output_spec or SpecInfo(name="Output", fields=output_fields, bases=(BaseSpec,))
        self.output_spec = output_from_inputfields(self.output_spec, self.input_spec)

        super().__init__(
            name=name,
            inputs=kwargs,
            audit_flags=audit_flags,
            cache_dir=cache_dir,
            cache_locations=cache_locations,
            cont_dim=cont_dim,
            messenger_args=messenger_args,
            messengers=messengers,
            rerun=rerun,
        )

    def _run_task(self):
        import datalad.api as dl

        in_file = self.inputs.in_file
        dataset_path = self.inputs.dataset_path

        # Check if the dataset is already downloaded
        if not (Path(dataset_path) / ".datalad").exists():
            try:
                dataset_url = self.inputs.dataset_url
                os.makedirs(dataset_path, exist_ok=True)
                dl.install(source=dataset_url, path=dataset_path)
            except Exception as e:
                raise e
        else:
            ds = dl.Dataset(self.inputs.dataset_path)

        # Get the file
        ds.get(self.inputs.in_file)

        # Check if the file was downloaded
        if not Path(dataset_path, in_file).exists():
            raise FileNotFoundError(f"File {in_file} not found in {dataset_path}")

        _pth = Path(in_file)
        if not _pth.is_absolute():
            _pth = dataset_path / _pth

        _datalad_candidate = _pth.is_symlink() and not _pth.exists()

        if _datalad_candidate:
            try:
                result = dl.get(_pth, dataset=dataset_path)
            except Exception as exc:
                raise exc
            else:
                if result[0]["status"] == "error":
                    raise RuntimeError(f"datalad get failed: {result}")

                self.output_ = None
        output = os.path.abspath(
            os.path.join(self.inputs.dataset_path, self.inputs.in_file)
        )
        output_names = [el[0] for el in self.output_spec.fields]
        if output is None:
            self.output_ = {nm: None for nm in output_names}
        elif len(output_names) == 1:
            self.output_ = {output_names[0]: output}
        elif isinstance(output, tuple) and len(output_names) == len(output):
            self.output_ = dict(zip(output_names, output))
        else:
            raise RuntimeError(
                f"expected {len(self.output_spec.fields)} elements, "
                f"but {output} were returned"
            )

    def _list_outputs(self):
        outputs = self.output_spec().get()
        outputs["out_file"] = os.path.abspath(
            os.path.join(self.inputs.dataset_path, self.inputs.in_file)
        )
        return outputs

