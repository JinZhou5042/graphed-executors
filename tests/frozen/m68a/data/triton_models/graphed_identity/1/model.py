"""Triton python-backend identity model served by the m68a CI container (tests/frozen/m68a).

``graphed_identity`` with the name and I/O the EAF Triton serves (P9): OUTPUT0 is INPUT0, FP32, any
shape. graphed's Triton plugin sends an (n_events, n_features) matrix, so both tensors are rank 2.
"""

import importlib
from typing import Any

pb_utils = importlib.import_module("triton_python_backend_utils")  # provided by the Triton python backend


class TritonPythonModel:
    def execute(self, requests: list[Any]) -> list[Any]:
        responses = []
        for request in requests:
            x = pb_utils.get_input_tensor_by_name(request, "INPUT0").as_numpy()
            out = pb_utils.Tensor("OUTPUT0", x.astype("float32"))
            responses.append(pb_utils.InferenceResponse(output_tensors=[out]))
        return responses
