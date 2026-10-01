Running on an HTCondor pool
===========================

If your site gives you an HTCondor pool — the LPC, lxplus, or one of your own — graphed can submit
its own worker jobs there and run your plan on them. You need the HTCondor Python bindings and
nothing else: no dask scheduler, no parsl interchange. The worker jobs (pilots) start, call back to
your session, pull tasks one at a time, and are removed when you close the runner.

This page is the how-to. For *why* the answer doesn't move when the pilot count does, read
:doc:`design`.

Try it on your laptop first
---------------------------

The same pilot program runs as local subprocesses, so you can check a plan's wiring before you
queue anything. This needs ``graphed-executors[htcondor]`` only for real jobs; the local run below
works with a plain install. Put the task functions in a module:

.. code-block:: python

    # my_tasks.py
    import numpy as np

    def count(partition, resources):
        return np.asarray([partition.entry_stop - partition.entry_start])

    def add(a, b):
        return a + b

    def zero():
        return np.zeros(1, dtype=int)

and run the plan through two local pilots:

.. code-block:: python

    from graphed.core import Partition, Plan, Task
    from graphed_executors.htcondor_backend import HTCondorBackend, HTCondorRunner, LocalPilots
    from my_tasks import add, count, zero

    parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
    plan = Plan(process=count, combine=add, empty=zero,
                tasks=tuple(Task(i, p) for i, p in enumerate(parts)))

    backend = HTCondorBackend(LocalPilots(), n_pilots=2, host="127.0.0.1")
    with HTCondorRunner(backend) as runner:
        result = runner.run(plan)
    print(result.value, result.n_partitions, result.n_combines)

Each pilot announces itself, then the result prints::

    pilot myhost:47041:5c0e9a1f serving http://127.0.0.1:10000
    pilot myhost:47040:b82d4e07 serving http://127.0.0.1:10000
    [700] 7 6

On a pool, only the runner line changes: ``htcondor_runner(site=..., n_pilots=...)`` below.


Read these four before your first pool run
------------------------------------------

.. warning::

   **Pilots import your task functions by name.** A pilot is a fresh Python process in a batch
   slot. It finds ``plan.process`` and ``plan.combine`` by module and name, so a function defined
   in the script you run (``__main__``) or a lambda cannot reach it. The runner checks before it
   submits anything and says so::

       ValueError: pilots import plan.process by name; move <function count at 0x…> into a
       module and pass it in `user_modules=[...]` (__main__.count)

   Put the functions in a file and pass that file in ``user_modules=[...]``; it is copied into
   each pilot's working directory, which is on the pilot's ``sys.path``.

.. warning::

   **Your partial results come back through your session.** Pilots never talk to each other: the
   driver hands each merge its two inputs. Every leaf result and every intermediate crosses the
   machine you run from, so keep a partial result small — a histogram is fine, a per-event array
   is not — or run the job on dask.

.. warning::

   **A pilot runs one task at a time**, whatever ``request_cpus`` asks for. Scale with
   ``n_pilots``, not with cores per pilot; raise ``request_cpus`` only when a single task is
   multithreaded itself.

.. warning::

   **A pilot that dies is noticed after 30 seconds** of silence. Its task runs again on another
   pilot; if that pilot dies too, the run fails with a ``StageError`` naming the partition and the
   last pilot (``host:pid:token``). If every pilot is gone and none is queued, the run fails with
   "no pilots left" instead of waiting.


Installing
----------

::

    pip install "graphed-executors[htcondor]"    # pulls htcondor>=25.13

The bindings ship wheels for Linux (x86_64 and aarch64) only, so the extra installs on a Linux
submit host or inside a Linux container. Importing :mod:`graphed_executors.htcondor_backend` does
not import the bindings; submitting does.


On the LPC
----------

Pilots run inside the coffea image, so the driver runs inside it too, and the pilots get your
driver's virtual environment shipped with them. Three steps, from a ``cmslpc-el9`` login node.

**1. A grid proxy.** The pilots authenticate with it::

    voms-proxy-init -voms cms -valid 192:00

The login nodes point ``X509_USER_PROXY`` at ``~/x509up_u<your uid>``, so that is where it lands, and
where the ``lpc`` site tells the schedd to find it.

**2. The driver inside the image.** Work under your 3-day scratch area — the LPC schedds read
your submit files only from there — and enter the image with the same binds lpcjobqueue's
``bootstrap.sh`` uses. The last two binds hide the login node's ``LOCAL_CONFIG_FILE`` from the
image; without them the bindings stop at ``ERROR: Can't read config source …``:

.. code-block:: bash

    # A recipe: this needs an LPC login node.
    IMAGE=/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-almalinux9-noml:2026.9.0-py3.12
    L=/usr/local/bin/cmslpc-local-conf.py
    WORK=$(mktemp -d -p /uscmst1b_scratch/lpc1/3DayLifetime/$USER graphed.XXXX)
    cd "$WORK"
    printf '#!/bin/bash\npython3 %s.orig | grep -v "LOCAL_CONFIG_FILE"\n' "$L" > .cmslpc-local-conf
    chmod u+x .cmslpc-local-conf
    export APPTAINER_BINDPATH=/uscmst1b_scratch,/cvmfs,/cvmfs/grid.cern.ch/etc/grid-security:/etc/grid-security,/etc/condor/config.d/01_cmslpc_interactive,$L:$L.orig,$WORK/.cmslpc-local-conf:$L
    export CONDOR_CONFIG=/etc/condor/config.d/01_cmslpc_interactive
    apptainer shell --pwd "$WORK" "$IMAGE"

**3. A venv the pilots can unpack.** Inside the image, make a venv on top of the image's packages
and install into it — never with ``pip install -e``: the pilots get a copy of the venv, and an
editable install points back at a source tree they do not have. The runner refuses an editable
install and names it.

.. code-block:: bash

    python -m venv --system-site-packages venv
    venv/bin/pip install "graphed-executors[htcondor]"     # add your analysis package here
    venv/bin/python my_run.py

``my_run.py`` builds the plan and hands it to ``htcondor_runner``:

.. code-block:: python

    # A recipe: this needs the LPC pool.
    from graphed.core import Partition, Plan, Task
    from graphed_executors.htcondor_backend import htcondor_runner
    import my_tasks

    IMAGE = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-almalinux9-noml:2026.9.0-py3.12"

    parts = tuple(Partition("data", "", i * 100, (i + 1) * 100) for i in range(7))
    plan = Plan(process=my_tasks.count, combine=my_tasks.add, empty=my_tasks.zero,
                tasks=tuple(Task(i, p) for i, p in enumerate(parts)))

    with htcondor_runner(site="lpc", n_pilots=2, image=IMAGE,
                         user_modules=[my_tasks.__file__]) as runner:
        print(runner.run(plan).value)          # [700]

What happens: the runner picks the least-loaded LPC schedd the way ``condor_submit`` does there,
submits one cluster of pilots with your venv and ``my_tasks.py`` spooled alongside, and waits for
the first pilot before it runs anything. On a September 2026 run, with about 12,700 jobs idle on
the chosen schedd, the first pilot connected 33 seconds after submission and the second after 116;
a busier pool queues pilots for as long as it queues anything else. Leaving the ``with`` block
retrieves the pilots' logs into ``log_dir`` and removes the jobs, which took 15 seconds.


On lxplus
---------

Enter the same image with ``/etc/condor`` and your Kerberos credentials bound, build the venv as
on the LPC, and pass ``site="lxplus"``. The ``lxplus`` site binds the task server to port 8786 on
the login node: batch nodes get ``Connection refused`` on the default range 10000–10100, and 8786 is
the one port CERN opens from workers to a submit host (for a dask scheduler), so **one driver per
login node**. A second ``htcondor_runner`` on the same node fails at once with ``OSError: no free
port for the task server: site=lxplus ports=8786-8786``; log in to another node. The first pilot was
live 105 s after submission on the run this is measured from
(``graphed-workdir/lanes/htcondor/probes/site-check-lxplus/transcript-8786.txt``; the refused
10000 run is ``pilot-logs-run2.txt`` next to it). The lxplus schedd refuses a spooled job that
brings nothing back, which the site handles with ``transfer_output_files=""``.

lxplus jobs carry your Kerberos credential (``SendCredential``) from the credd, which drops it an
hour after your last job leaves the queue (``SEC_CREDENTIAL_SWEEP_DELAY``); a job submitted while
it holds none never starts. So a submit that finds none stores one first, as ``condor_submit`` does.
Inside the image that fails, since CERN's credential producer is not in it, with a ``RuntimeError``
naming the command to run on the login node before you try again:
``$(condor_config_val SEC_CREDENTIAL_PRODUCER) | condor_store_cred add-krb -i -``.

Pilots run in the ``longlunch`` queue (two hours); pass
``extra_submit={"+JobFlavour": '"workday"'}`` for a longer run, or
``extra_submit={"output_destination": "root://eosuser.cern.ch//eos/user/..."}`` to have the logs
written to EOS instead of retrieved.


On any other pool
-----------------

``site="generic"`` submits to your default schedd with no site keys. The pilots run your driver's
Python directly, so the submit host and the execute nodes need to share that interpreter and its
packages (a shared filesystem, or the same image):

.. code-block:: python

    # A recipe: this needs an HTCondor pool.
    from graphed_executors.htcondor_backend import htcondor_runner

    with htcondor_runner(n_pilots=10, user_modules=["my_tasks.py"]) as runner:
        result = runner.run(plan)              # the plan from the laptop run above

For a site of your own, describe it once as a ``SiteProfile`` and pass that as ``site=``:

.. code-block:: python

    from graphed_executors.htcondor_backend import SiteProfile

    mysite = SiteProfile(
        name="mysite",
        submit={"MY.SingularityImage": '"{image}"', "+AccountingGroup": '"group_physics.{user}"'},
        spool=True,             # the schedd cannot read your submit directory
        ship_env=True,          # send the driver's venv along as env.tgz
        sandbox_root=None,      # or a directory the schedd can read, which log_dir must sit under
        schedd_query=None,      # or (param naming the collectors, constraint) to pick a schedd
        driver_ports=(10000, 10100),  # what the execute nodes can reach on the submit host
    )

Submit values may use ``{image}``, ``{uid}``, ``{user}`` and ``{home}``.


Running without a login session
-------------------------------

``htcondor_runner`` needs your session alive for the whole run: the pilots call back to it. For a
run that should outlive your login, ``submit_driverless`` puts the driver itself in a job. It
pickles the plan, writes ``plan.pkl`` and ``run.json`` into ``log_dir``, and submits **one** job
that runs ``python -m graphed_executors.htcondor_backend.driver``; you get a ``RunHandle`` back and
can log out.

.. code-block:: python

    # A recipe: this needs the LPC pool, from the image and venv of the steps above.
    import os
    from graphed_executors.htcondor_backend import submit_driverless

    work = os.getcwd()                          # under your 3-day scratch area
    handle = submit_driverless(plan, site="lpc", image=IMAGE, n_pilots=8,
                               request_memory_mb=16000, log_dir=work,
                               user_modules=[my_tasks.__file__])
    handle.save("run-handle.json")

Later, from any session on the same pool:

.. code-block:: python

    from graphed_executors.htcondor_backend import RunHandle

    handle = RunHandle.load("run-handle.json")
    print(handle.status())                      # queued, running, held, done, failed or removed
    handle.wait(timeout=3600)                   # returns on done, failed or removed
    result = handle.result()                    # the ExecResult; a failed run re-raises its error
    handle.remove()                             # the LPC and lxplus keep a completed job queued

Where the pilots run depends on ``pilots=``:

* ``pilots="local"`` (the default) asks for one slot with ``n_pilots`` CPUs and runs the pilots
  inside it as subprocesses of the driver. This is the only choice on the **LPC**, whose jobs
  cannot submit jobs (``SiteProfile.jobs_can_submit``); size ``request_memory_mb`` for all of
  them together.
* ``pilots="condor"`` has the driver job submit ``n_pilots`` pilot jobs of its own, to the schedd
  your session chose, and host its task server on one of the site's ``worker_ports`` (10000–10100
  on every built-in site). The pilots need a submit directory the schedd reads, so ``log_dir`` must
  lie under the site's ``job_root`` (``SiteProfile.job_root``: ``/afs`` on **lxplus**, any path on
  ``generic``); anything else is refused, and so is a site without a ``job_root`` or without
  ``worker_ports``.

The job brings back ``result.pkl`` and ``driver.log`` (the pilots' pids or the pilot cluster,
timings, and any traceback); on the LPC and lxplus a single job is spooled and ``result()``
retrieves them into ``log_dir`` (a run with service nodes is not spooled; see `Cluster-hosted
services`_). ``logs()`` returns the driver's log files that have come back. The job's
exit code decides whether HTCondor runs it again:

.. list-table::
   :header-rows: 1
   :widths: 8 72 20

   * - Exit
     - Meaning
     - Retried
   * - 0
     - The plan ran.
     - No
   * - 1
     - Anything another attempt may get past: the run's workers were lost (every pilot preempted,
       say), pilots could not start, a service the plan needs could not be reached or started
       (see `Services`_), or the driver failed before or after the run.
     - Twice
   * - 3
     - The plan's own code raised; it would raise again.
     - No
   * - killed
     - The driver died before writing its result: out of memory, a signal, no interpreter in the
       image. The job's script writes a placeholder ``result.pkl`` and ``driver.log`` before Python
       starts, so the job still completes (a job whose declared outputs are missing would be held).
       If the last try dies too, ``result()`` raises ``RuntimeError: the driver exited before writing
       a result; see driver.log``.
     - Twice

On a spooled site (the LPC, lxplus) the completed job stays in the queue after ``result()``, so
call ``handle.remove()``; it also stops a job that is still running. ``wait()`` returns only on
done, failed or removed, so ``wait(timeout=None)`` does not return while the job is held: pass a
``timeout``, or check ``status()`` for ``held``. Everything a pilot cannot import (a lambda, a
function defined in ``__main__``) is refused before anything is written or submitted, as for
``htcondor_runner``.


Services
--------

Some analyses call a server while they run: an inference server a node sends its rows to, say.
The analysis declares each one as a ``graphed.services.ServiceSpec`` (a name, a ``kind`` and a
readiness ``check``, with an optional ``Launch`` recipe that starts one), and its nodes name it; the
plan carries the specs it references as ``plan.services``. Where the service is — its *endpoint*,
``scheme://host:port`` — is not part of the analysis: the runner finds one for each run.

It tries three places, in order:

1. **An endpoint you give it**: ``htcondor_runner(..., services={"triton": "grpc://host:8001"})``,
   ``SubmitRunner(backend, services=...)``, or ``submit_driverless(..., services=...)``. If it fails
   its check the run is refused, naming it: an endpoint you asked for is never swapped for another.
2. **The site's**: ``SiteProfile.services`` maps a ``kind`` to the endpoint the site hosts it at.
   The ``lpc`` row names the Elastic Analysis Facility's inference server
   (``grpcs://triton.fnal.gov:443``); ``lxplus`` and ``generic`` host none. A site endpoint that
   fails its check is passed over, and the reason is kept.
3. **One it starts**, from the spec's recipe: beside the driver when the recipe needs no image and
   no GPU and the site lets workers reach the driver's host (``service_hosts``), else as a job of its
   own on the pool (see `Cluster-hosted services`_). It is stopped when the run ends.

``graphed_executors.submit.recipes`` has two recipes as plain data: ``triton(name, image,
model_repository)`` (gRPC only, one port) and ``http_server(name)`` (Python's ``http.server``).

**Wires and checks.** The endpoint's scheme is the wire: ``tcp``, ``http``, ``https``, ``grpc`` or
``grpcs`` (the ``s`` ones are TLS); a bare ``host:port`` is refused. The spec's ``check`` is ``"tcp"``
(a connect), ``"http:<path>"`` (a GET that must answer 2xx, and not with a gRPC content type) or
``"grpc:<service>"`` (the standard gRPC health check, which needs ``grpcio``). A check runs only over
a wire that can carry it: an ``http:`` check against a ``grpc://`` endpoint fails without dialling,
because a gRPC gateway answers HTTP 200 to every path.

**Checked from a worker.** Passing the check on the driver's host proves little about the execute
nodes, so before the plan's first task the runner submits a small probe task that runs the same
checks from a pilot: one task checks every service the run needs, and the runner waits for its
answer up to the largest ``timeout_s`` among those services. A service the runner started must also
answer a pilot on another host than its own, unless that host is the driver's (a run on one
machine). A service no pilot can reach fails the run with ``ServiceUnreachable`` naming the endpoint, the worker and the reason; ``"no worker
answered"`` means no pilot ran the probe within that wait, and ``"only same-host
workers answered"`` that none on another host did.

**Kept warm across plans.** A started service lives as long as the run that started it. To use one
server for several plans, start it yourself and pass its endpoints to the runner:

.. code-block:: python

    from graphed_executors.submit.services import ServiceSet

    with htcondor_runner(site="generic", n_pilots=4) as runner:
        runner.wait_for_pilots()
        with ServiceSet(plan.services, runner.backend) as endpoints:
            runner.services = endpoints
            first = runner.run(plan)
            second = runner.run(other_plan)

Each run logs how it satisfied every service on the ``graphed_executors.services`` logger (the
record's ``status`` attribute is a ``ServiceStatus``: leg, endpoint, host, times). A driverless job
resolves the services in the driver job, by the same three places, with the job's own site row, and
runs a service that needs an image or a GPU as a node of its DAG (below); a service it cannot reach
or start exits 1, so HTCondor retries it.

Cluster-hosted services
~~~~~~~~~~~~~~~~~~~~~~~

A recipe that needs an image or a GPU cannot run beside the driver, so the runner runs it as a job
of its own on the pool, next to the pilots, for the length of the run. Every built-in site allows
this (``"cluster"`` is in ``service_hosts`` wherever ``worker_ports`` is set). The job starts a small
standard-library script (Python 3.9 or later, so it runs in an image with no venv) that takes the
first free port of the site's ``worker_ports``, starts the recipe's command on it, runs the spec's
check there, and then *announces* ``host:port`` to the runner's task server, signed with a secret
made for that one service. The runner waits up to the spec's ``timeout_s`` for the announce. A job
that ends or is held first raises ``RuntimeError`` naming its state and its directory
``service-<key>/`` under ``log_dir``; a job that has not announced by then raises ``TimeoutError``;
either way the job is removed. When the run ends the job is removed too, and leaves its slot (and its
GPU) within 30 seconds. On a spooled site (the LPC, lxplus) a service job that completed has its
``service.out`` and ``service.err`` fetched into ``service-<key>/`` when it is removed.

Three things to know when you write or use a recipe:

* **The command runs in a directory that holds exactly the recipe's** ``inputs``, each under its
  basename (a directory keeps its tree). Nothing else from the job's scratch directory is there —
  not your credentials, not the job's own files — so a recipe names its inputs by basename. Pass
  ``triton(..., model_repository="models")`` for a ``models/`` directory in the directory you submit
  from, never a path such as ``data/models``. Inputs are found relative to where you submit from; one
  that is missing, two that share a basename, and one that is or holds a symlink to a directory are
  refused before anything is submitted.
* **A custom recipe's server must bind with** ``SO_REUSEADDR``. A port counts as free when a bind
  with ``SO_REUSEADDR`` succeeds, so a server that binds without it can fail on a port that holds
  only a ``TIME_WAIT``; the job then exits instead of trying the next port. ``http.server`` and gRPC
  servers (Triton among them) already set it.
* **In** ``argv``, ``{python}`` is the job's interpreter (the shipped venv's, or ``python3`` in an
  image), ``{port}`` the chosen port and ``{host}`` the node's name.

**In a driverless run.** A driver job does not submit service jobs itself. When a plan has a service
that needs an image or a GPU, and neither ``services=`` nor the site's row serves it,
``submit_driverless`` submits a DAG instead of one job: a ``driver`` node, which is the job described
in `Running without a login session`_, and one ``SERVICE`` node per such service, named ``svc0``,
``svc1``, … in the order of the services' names. The driver writes its URL and a secret made for
the announces into the run directory; each service node announces to whichever try of the driver is
running, so a retried driver finds the same service again. DAGMan removes the service nodes when the
DAG ends. Each such run gets a new directory, ``<log_dir>/graphed-<nonce>/``, which is the handle's
``log_dir``: ``result.pkl``, ``driver.log``, the node files and DAGMan's ``run.dag.dagman.out`` are
there. ``status()`` reports the driver node (``held`` while it is held), and the outcome is the last
try's exit; a DAG whose driver never returned a result raises ``RuntimeError`` naming
``run.dag.dagman.out``.

The DAG is not spooled: the schedd and the nodes read the run directory, your ``user_modules`` and
the services' inputs where they lie, so all of them must lie under the site's ``job_root``
(``SiteProfile.job_root``: ``/afs`` on lxplus, any path on ``generic``). Anything outside is
refused before anything is submitted. The LPC has no ``job_root``, so a driverless LPC run refuses a
service that would need a node; the inference server its row names still serves that kind (leg 2).
The check reads the path as written, not where a symlink points: an input under the root that is a
symlink to a file outside it passes, and if the schedd cannot read that file the node is held. A held
service node is removed (its ``periodic_remove``); each try of the driver then waits ``timeout_s``
for its announce, so the run fails after three × ``timeout_s``, as it does for a service node that
starts but never announces. A held driver node stays ``held`` until you release or remove it.

**On lxplus, with a GPU.** An inference server for your ONNX models in ``models/`` (Triton's layout),
reached from CPU pilots:

.. code-block:: python

    # A recipe: this needs lxplus, from a directory under /afs that holds models/.
    import os
    from graphed_executors.htcondor_backend import htcondor_runner, submit_driverless
    from graphed_executors.submit import recipes

    triton = recipes.triton(
        "triton",
        image="/cvmfs/unpacked.cern.ch/nvcr.io/nvidia/tritonserver:24.11-py3",
        model_repository="models",               # a basename: the server runs where its inputs land
    )
    # the analysis declares it as a service its nodes call; with the plan built:
    with htcondor_runner(site="lxplus", n_pilots=2, image=IMAGE,
                         extra_submit={"+JobFlavour": '"espresso"'}) as runner:
        result = runner.run(plan)                # the server runs as a GPU job beside the pilots

    handle = submit_driverless(plan, site="lxplus", image=IMAGE, n_pilots=2,
                               request_memory_mb=4000, log_dir=os.getcwd())   # one SERVICE node


The arguments you will change
-----------------------------

.. list-table::
   :header-rows: 1
   :widths: 26 74

   * - Argument
     - What it does
   * - ``n_pilots``
     - How many pilot jobs to submit. One pilot runs one task at a time.
   * - ``site``
     - ``"lpc"``, ``"lxplus"``, ``"generic"`` (the default), or a ``SiteProfile``.
   * - ``image``
     - The container image; ``lpc`` and ``lxplus`` need one.
   * - ``user_modules``
     - Files or package directories copied next to each pilot, importable there by name.
   * - ``request_cpus``, ``request_memory_mb``
     - Per pilot; 1 and 2048 by default.
   * - ``log_dir``
     - Where ``pilot.<n>.out``, ``pilot.<n>.err`` and ``pilots.log`` land. By default a fresh
       directory under the site's scratch area, or a temporary one.
   * - ``env``
     - The venv to ship on sites that ship one; your driver's own by default.
   * - ``extra_submit``
     - Submit keys added last, so they override the site's.
   * - ``host``
     - The name pilots call back to; this machine's fully qualified name by default.
   * - ``port_range``
     - The driver-side ports the task server may bind, inclusive; the first free one is used. By
       default the site's ``driver_ports``: 10000–10100 on ``lpc`` and ``generic``, 8786 on
       ``lxplus``. A range with no free port is an ``OSError`` naming the site and the range.
   * - ``min_pilots``
     - How many pilots must be connected before the first run starts; 1 by default.
   * - ``retries``
     - Has no effect here. The only retry is the one re-run of a task whose pilot was lost.


Temporary space on the worker
-----------------------------

No code in graphed-executors names ``/tmp`` or ``/var/tmp``. A pilot or driver job unpacks its venv
and writes its logs in its working directory, which is the job's scratch directory, and anything
made with Python's ``tempfile`` goes to ``TMPDIR``.

**On the LPC** the scratch directory is ``/srv`` inside the container, on the node's local disk, and
``TMPDIR``, ``TMP`` and ``TEMP`` point at it; a submit ``environment`` does not move ``TMPDIR``. ``/tmp``
and ``/var/tmp`` share one 64 MiB ``tmpfs`` made by the site's container wrapper, so a tool that
writes to either itself runs out of space at 64 MiB between them (September 2026, ``graphed-workdir/lanes/htcondor/probes/site-lpc/m68b-tmpenv.txt`` and
``m68b-proxy-scratch.txt``). Point such a tool at the scratch directory with its own variable, and
ask for the space with `request_disk
<https://htcondor.readthedocs.io/en/latest/man-pages/htcondor-jdl.html#request_disk>`__:

.. code-block:: python

    extra_submit = {"environment": '"MYTOOL_TMP=/srv"', "request_disk": "20G"}

Pass it to ``htcondor_runner`` or ``submit_driverless``. The submit ``environment`` reaches the job
and ``extra_submit`` is applied after graphed-executors' own keys, none of which is
``environment``. ``submit_driverless`` gives them to every job of the run: the driver job, the
pilot jobs it submits and any service nodes.


When something goes wrong
-------------------------

* **No pilot starts.** After ten minutes the first run raises ``RuntimeError: 0 of 1 pilots
  connected after 600.0s; see the pilot logs in <log_dir>``. Look at ``pilots.log`` there for
  holds, and at ``pilot.0.err`` for a pilot that started and could not reach you — the port range
  has to be open from the execute nodes to your submit host.
* **A pilot dies mid-task.** It is re-run once elsewhere; the second death fails the run with a
  ``StageError`` whose ``partition`` is the chunk and whose message names ``host:pid:token``.
* **A pilot exits with code 2** and prints "wrong secret file": it was pointed at another run's
  task server. Each run has its own secret.
* **A pilot outlives your session.** Once your session has been unreachable for 30 seconds, the
  pilot stops at once, even in the middle of a task, so a crashed driver does not hold batch slots.


Who can talk to the task server
-------------------------------

Each run makes a 32-byte secret, writes it to a file only you can read, and ships the file with
the pilots; it is never in the job's arguments or environment, which anyone who can query the
queue can read. Every request a pilot makes is signed with it, and the task server refuses an
unsigned or wrongly signed request before it reads the request's contents. The traffic itself is
plain HTTP, so tasks and results are readable on the wire: run inside your site's network.
