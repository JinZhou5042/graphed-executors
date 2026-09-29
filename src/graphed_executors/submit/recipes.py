"""Launch recipes as plain data: constructors of graphed ``ServiceSpec`` requirements with a ``Launch``.

This is the only executors module that names a service. Nothing here runs anything: a recipe is data
the analysis declares (``Session.declare_service``), and the engine's service set starts it where the
backend can (``submit/services.py``). Argv templates are rendered where the service starts: ``{port}``
the bound port, ``{host}`` the advertised host, ``{python}`` that side's interpreter.
"""

from __future__ import annotations

from graphed.services import Launch, ServiceSpec


def triton(name: str, image: str, model_repository: str, *, gpus: int = 1) -> ServiceSpec:
    """A Triton inference server (kind ``"triton"``) in ``image`` serving ``model_repository`` (a path
    relative to where the job lands its inputs) on gRPC only: one bound port, checked with the standard
    gRPC health Check (``ServerIsReady``-backed), because a site's Triton that must satisfy the same
    requirement serves only gRPC."""
    launch = Launch(
        argv=(
            "tritonserver",
            f"--model-repository={model_repository}",
            "--grpc-port={port}",
            "--allow-http=false",
            "--allow-metrics=false",
        ),
        image=image,
        inputs=(model_repository,),
        resources={"gpus": gpus},
    )
    return ServiceSpec(name, "triton", check="grpc:", launch=launch)


def http_server(name: str, *, root: str = ".") -> ServiceSpec:
    """The generic recipe: stdlib ``http.server`` (kind ``"http"``, checked by a GET of ``/``) serving
    ``root``, the directory it starts in by default. Any other ``root`` is appended as
    ``--directory <root>`` and is the recipe's staged input, for a host that ships inputs to where the
    service starts. Every hosting path runs it without a service client."""
    argv = ("{python}", "-m", "http.server", "{port}")
    if root == ".":
        return ServiceSpec(name, "http", check="http:/", launch=Launch(argv=argv))
    launch = Launch(argv=(*argv, "--directory", root), inputs=(root,))
    return ServiceSpec(name, "http", check="http:/", launch=launch)


__all__ = ["http_server", "triton"]
