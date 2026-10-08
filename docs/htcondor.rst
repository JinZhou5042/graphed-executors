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
``htcondor_runner``; a join or repartition plan's stage functions travel by value instead, so they
are accepted.


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
   own on the pool (see `Cluster-hosted services`_). It is stopped when the run ends. On Linux, a
   start beside the driver whose port's listener the started process does not hold (another process
   bound the port after the free-port scan) is refused, since a check passes on any listener.

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

**Checked again when a task fails.** A task that raises runs the same checks on its pilot. A service
that no longer answers turns the failure into ``ServiceUnreachable``, whose reason ends with the task's
own exception; with every service answering, the task's exception is raised as it was. Either way
the run ends at its first failed task and cancels the tasks still queued.

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
check there (on Linux, a port whose listener the command's processes do not hold is passed over for
the next), and then *announces* ``host:port`` to the runner's task server, signed with a secret
made for that one service. While the job waits for a slot (idle, or spooling its inputs) the runner
waits with it, with no deadline (`Schedulability`_), and logs the job's key and
state on the ``graphed_executors`` logger at first and every 30 seconds; the spec's ``timeout_s``
counts from the job's start (its first ``JobStatus == 2``), and an eviction back to idle does not
restart it. A job that ends or is held first raises ``RuntimeError`` naming its state and its
directory ``service-<key>/`` under ``log_dir``; a job that has started and not announced within
``timeout_s`` raises ``TimeoutError``; either way the job is removed. When the run ends the job is removed too, and leaves its slot (and its
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

A service node restarts a command that dies, up to three times, each on a port none of its
earlier ones served, and announces the new endpoint. A task that then finds the service gone ends the
run with ``ServiceUnreachable``, and the driver runs the plan again in the same job, against the new
announce and on the same pilots (``driver.log`` says ``rerun:``); the tasks that had finished run
again.

While the driver waits for an announce it reads the node's queue ad on the DAG's schedd. A node that
is held fails the try at once, naming its ``HoldReason``; so does one that left the queue, naming the
``LastHoldReason`` (else the ``RemoveReason``) of its history row; so does an idle node no slot of
the pool could ever run, counting the slots the run's own running jobs (and its running pilots) hold,
named with its ``RequestMemory``, ``RequestCpus`` and ``RequestGPUs``. An idle node that could run
waits without a deadline, and ``timeout_s`` counts from its start. With ``pilots="condor"``, the
run's queued pilots are held while a node waits, and released after. Where the schedd cannot be read,
``driver.log`` says so in one line and the wait is ``timeout_s`` long.

The DAG is not spooled: the schedd and the nodes read the run directory, your ``user_modules`` and
the services' inputs where they lie, so all of them must lie under the site's ``job_root``
(``SiteProfile.job_root``: ``/afs`` on lxplus, any path on ``generic``). Anything outside is
refused before anything is submitted. The LPC has no ``job_root``, so a driverless LPC run refuses a
service that would need a node; the inference server its row names still serves that kind (leg 2).
The check reads the path as written, not where a symlink points: an input under the root that is a
symlink to a file outside it passes, and if the schedd cannot read that file the node is held. A held
service node is removed (its ``periodic_remove``), so each try of the driver that reads the schedd
fails at once and the run fails after three tries; a service node that starts but never announces
costs each try ``timeout_s``.
A held driver node stays ``held`` until you release or remove it.

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


Schedulability
~~~~~~~~~~~~~~

A service starts only where it can run: a run that cannot place one anywhere is refused before any
of its work starts. A run whose service can run on some slot waits for one, with no deadline, for as
long as other jobs hold the room; while it waits, the ``graphed_executors`` logger names the job's key
and state every 30 seconds. The runner's queued pilots never take that room:

* **A run's service jobs go before its pilots.** A runner whose services are jobs submits its pilots
  at the first need of a worker (a task, ``n_workers()`` or ``wait_for_pilots()``, from any plan);
  a need that comes while a plan's services are starting is put off until they have all started (or
  one failed), so the pilots queue for the room beside them. A driver job's pilots likewise follow its
  ``SERVICE`` nodes' announces. That first need waits up to ten minutes for ``min_pilots``, counted
  from the pilots' submit.
* **A later plan's servers wait with the runner's queued pilots held**, across all of that plan's
  services, and the pilots are released once those services have started or failed; a hold you
  placed yourself is left alone.
* **One plan's services start at a time.** Plans of one runner overlap (a ``submit`` beside a
  ``run``, or ``run`` from two threads), but their service sets start one after another, each through
  its worker probe, so no plan's server waits for room that another plan still starting holds. A plan
  waiting its turn waits as long as the starting plan waits for its slot, even when its own servers
  would fit now, and raises ``RuntimeError`` naming the close once the runner closes or stops waiting.
* **Closing.** ``runner.close()``, or leaving the ``with`` block normally, waits for every submitted
  plan, a server still waiting for its slot included. Ctrl-C anywhere (in the ``with`` block, or in
  ``close()`` itself) ends the wait and removes the run's jobs, its servers and its pilots. Any other
  exception leaving the ``with`` block ends the wait too: the job is removed and the plan raises
  ``RuntimeError`` naming its key. From then on no pilot is submitted, and a wait for pilots that
  were never submitted raises naming the close; held pilots are still released for the plans the
  close still finishes.

* **Where it may go.** ``htcondor_runner(..., service_hosts=("cluster",))`` (or ``("driver",)``)
  narrows the site's ``service_hosts`` for this run. A host the site does not offer is a
  ``ValueError`` naming the ones it does, before any pilot is submitted; a driver job offers only
  ``("driver",)``.
* **Beside the driver, it must fit.** A recipe's ``resources["memory_mb"]`` (none counts as 0),
  summed over the run's services beside the driver, must fit ``backend.driver_memory_mb`` if the
  backend sets it, else the driver host's physical memory. Inside a driver job,
  ``HTCondorBackend.driver_memory_mb`` is the slot's ``Memory``, since the services share the job's
  slot. A service that does not fit goes to the cluster, its status ``detail`` naming the sizes; a
  backend with no cluster host refuses it with ``ServiceUnavailable``, whose ``legs["managed"]`` names
  the sizes and the limit.
* **On the cluster, a slot must match.** After the service job is submitted, its ad is matched against
  every slot of the pool, busy or not, with a partitionable slot counted whole (its total memory, CPUs,
  GPUs and disk) less what the runner's running pilots and the run's earlier servers hold there: the
  pilots keep their slots until the runner closes, the servers until the run ends. A server that would
  fit only where they sit is removed before it runs, and the run raises ``ServiceUnavailable`` naming
  their clusters. A job no slot matches (too much memory, a requirement no machine meets) is removed
  before it ever runs, and the run raises ``ServiceUnavailable`` naming ``RequestMemory``,
  ``RequestCpus``, ``RequestGPUs`` and the largest slot's memory. A job that matches a busy slot waits
  for it, as above. A collector that lists no slot at all is not asked: the job is submitted and waits.

Histograms on histserv servers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``graphed_histogram.histserv`` fills histograms on `histserv <https://pypi.org/project/histserv/>`__
servers instead of shipping each task's partial histograms through the reduction. A ``Context`` sizes
the histograms and packs them onto servers of the sizes you offer; each server is a service in
``plan.services``, with its size as ``resources["memory_mb"]``. The runner then starts, checks, binds
and stops them as it does any service, and resolves the histograms out of the servers while they are
up, so the value holds plain ``boost_histogram.Histogram`` objects. Install the
``graphed-histogram[histserv]`` extra; histserv needs ``grpcio``, which has no free-threaded wheel.

.. code-block:: python

    import awkward as ak
    import boost_histogram as bh
    import graphed_histogram as gh
    from graphed import Session
    from graphed.awkward import AwkwardBackend, from_parquet
    from graphed_histogram import histserv
    from graphed_executors.submit import SubmitRunner, ThreadBackend

    ak.to_parquet(ak.Array({"x": [0.5, 1.5, 2.5, 3.5] * 100}), "x.parquet")
    events = from_parquet(Session(AwkwardBackend()), "events", "x.parquet", steps_per_file=4)
    ctx = histserv.Context(memory_mb=512, workers=2, name="docs-htcondor")
    h = histserv.Histogram(bh.axis.Regular(4, 0.0, 4.0), context=ctx)
    h.fill(events.x)
    plan = gh.plan({"x": h}, steps_per_file=4)
    print([(spec.name, dict(spec.launch.resources)) for spec in plan.services])

    with SubmitRunner(ThreadBackend(2)) as runner:
        print(runner.run(plan).value["x"].values())

Printed output::

    [('docs-htcondor-0', {'memory_mb': 512})]
    [100. 100. 100. 100.]

``workers`` is the number of tasks that fill at once (here the two threads; on a pool, the pilots).
On a pool the server runs beside the driver when it fits there and the site lets workers reach the
driver, else as a job of its own sized to the server (`Cluster-hosted services`_). Pass
``service_hosts=("cluster",)`` to keep servers off a login node, or ``("driver",)`` to keep them on
it. A driverless run's servers run inside the driver job, so ``request_memory_mb`` must cover them
as well as the driver and its local pilots.

An H→γγ run
~~~~~~~~~~~

``examples/hgg/run_lpc.py`` runs the H→γγ example (:doc:`hgg`) at the LPC over the HiggsDNA 2024
sample manifests (``{dataset: [file, ...]}`` JSON), with its diagnostics on histserv servers. From a
login node, with a proxy and a venv that holds this package's ``[htcondor]`` extra,
``graphed-histogram[histserv]``, the coffea fork and ``higgs_dna``:

.. code-block:: bash

    python examples/hgg/run_lpc.py samples_2024_mc.json samples_2024_data.json --files 2 --parts 4 \
        --pilots 8 --server-mb 512 --env "$VIRTUAL_ENV"

It takes each dataset's first ``--files`` files (``GluGluHto2G_M-125_amcatnlo_2024`` and
``DataC_2024`` unless ``--datasets`` names others), splits each into ``--parts`` entry ranges, and
writes the parquet parts to ``--out``, by default ``root://cmseos.fnal.gov//store/user/<you>/hgg/``,
which the pilots write over xrootd with the job's proxy: graphed opens an ``--out`` holding ``://``
through fsspec, so a ``root://`` one needs fsspec-xrootd beside the analysis (the coffea image has it).
It prints the servers the context opened,
logs each server's status (where it ran, when it was submitted and ready), prints the counters, and
saves the diagnostics as UHI JSON (``--histograms``, ``hgg_diagnostics.json`` by default; read them
back with ``json.load(f, object_hook=uhi.io.json.object_hook)`` and ``boost_histogram.Histogram``).
``--placement cluster`` (the default) runs each server as a job of its own; ``--placement driver``
runs them on the login node. ``--driverless`` submits the whole run as one job whose slot holds the
driver, the ``--pilots`` local pilots and the servers, and asks for memory for all of them.


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
     - How many pilots must be connected when a run first needs a worker (its first task, or the
       check of its services from a worker); 1 by default.
   * - ``retries``
     - Has no effect here. The only retry is the one re-run of a task whose pilot was lost.
   * - ``service_hosts``
     - Where services the runner starts may run: a subset of the site's ``("driver", "cluster")``
       (`Schedulability`_).


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
