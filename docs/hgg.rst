Example: an H→γγ analysis, translated
=====================================

``examples/hgg/analysis.py`` is a coffea processor rewritten for graphed: ``inclusive_processor.py``,
a standalone distillation of HiggsDNA's H→γγ inclusive base processor. It covers the lumi mask, MET
filters and triggers, photon preselection, diphoton building, the detector-level fiducial cut, the
cleaned jet variables, and the particle-level truth and fiducial flags. For each chunk it writes one
flat parquet file of diphoton candidates and counts the chunk's events and weights. One plan runs
it over a whole fileset, MC and data together, and returns each dataset's summed counters.

The translation keeps the original's methods, names and cut values. Events are coffea NanoEvents in
graphed mode:

.. code-block:: python

   events = NanoEventsFactory.from_root(
       files,  # {file: {"object_path": "Events", "steps": [[start, stop], ...]}}
       schemaclass=NanoAODSchema,
       mode="graphed",
       metadata={"dataset": dataset},
   ).events()

The analysis reads the same files the original reads: HiggsDNA's ``infer_nano_version``, plus the
metaconditions, golden JSON and jet-ID JSONs, found through ``importlib.resources`` inside the
installed ``higgs_dna``. ``plan(fileset, year=..., out=...)`` takes coffea's
``{dataset: {file: {"object_path": "Events", "steps": [[start, stop], ...]}}}`` and builds one task
per step of every file. Any runner in this package runs it.

Data and MC take different branches in Python while the processor records (the lumi mask, the
weights, the truth columns), so each dataset records its own graph. ``dataset_plan`` makes one
``graphed.aggregate_plan`` per dataset: its outputs are the counters, and its one write is
``graphed.awkward.parquet_write`` of the flat record, whose part metadata holds that chunk's own
weight sums. ``graphed.collate`` joins the datasets' plans into the one plan ``plan`` returns. Each
task reads its chunk once, writes its part, and returns its counters, and the runner tree-reduces
the counters with ``coffea.processor.accumulate``, as coffea's ``Runner`` does. The value is
``{dataset: counters}``. ``plan({dataset: files})`` runs one dataset on its own, so datasets can
be submitted separately, and a dict union of their values is the same result.


What changed, and why
---------------------

Nothing is read when the processor runs, so wherever the original needs a value on the spot, the
translation has to spell that step another way. These are all eleven such places:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Original
     - Translation
   * - ``int(ak.num(...))``, ``len(events)``
     - The counters (``nTot``, ``nPos``, ``nNeg``, ``genWeightSum``, and the two weight sums in the
       metadata) are plan outputs, computed per chunk. ``Counters`` converts each counter to the
       original's Python type (``int`` or ``float``); the part's metadata is the ``str`` of each
       float32 sum.
   * - ``.to_numpy()``
     - The same counters. The old-NanoAOD supercluster-η projection uses the deferred PV columns
       directly.
   * - ``array["field"] = value``
     - ``gak.with_field(array, value, "field")``.
   * - ``ak.Array({...})`` for the flat output record
     - ``gak.zip`` of the flat columns.
   * - ``ak.to_arrow_table`` and ``pq.write_table`` inside ``process``
     - A ``parquet_write`` beside the counters, in the same pass: ``extensionarray=False``, the
       record's fields zipped in sorted order, and the part's key-value metadata replacing the
       schema's (the original's ``pa.table`` rebuild drops the schema's own).
   * - ``events.attrs["@events_factory"]._partition_key`` for the part name
     - ``part_name`` builds the name from the task's partition:
       ``<file stem>_Events_<start>-<stop>.parquet``. The original has the file's UUID where this
       has the file's stem. A file without explicit ``steps`` is refused, since a blind partition
       has no range to name its part by (and the counters depend on the chunking).
   * - ``LumiMask(path)(events.run, events.luminosityBlock)``
     - ``lumi_mask(run, lumi, year)``, an External recorded through
       ``graphed.preserve.externals.record_external``. Its payload is the golden JSON's bytes and
       its content hash is their SHA-256. Its evaluator is coffea's ``LumiMask`` on each chunk's
       run and lumi values. The plan therefore carries the certification it applied.
   * - ``correctionlib.CorrectionSet.evaluate`` for the jet ID
     - ``gak.apply_correction`` with a call template. The JSON is gunzipped first, because the
       correctionlib External parses JSON bytes.
   * - ``numpy.where``, ``numpy.copy``
     - ``gak.where``. The copy is dropped, since deferred arrays are never modified in place.
   * - ``if ak.num(diphotons.pt, axis=0) > 0:`` guards around jet cleaning
     - Deleted. The cleaning ops are defined on an empty chunk and give the same empty masks.
   * - ``ak.with_name(..., "PtEtaPhiMCandidate")``, which needs ``charge``
     - Unchanged. The records are zipped with ``charge`` already, and graphed NanoEvents carry
       coffea's candidate behaviors into each task.



How the translation is checked
------------------------------

The frozen suite ``tests/frozen/m69a`` uses the original script as the oracle. It imports the
file, kept byte-identical and pinned by SHA-256, and calls its ``process()`` on NanoEvents built
the way the script's ``__main__`` builds them, one chunk per range. It then compares each part the
translation writes against the original's part for the same range. ``compare_part`` covers:

* the counters, by ``==`` and by each value's Python type;
* the arrow schema: names, order, types and nullability;
* each column's validity bitmap;
* each column's valid values, bit for bit, with floats compared through their same-width
  unsigned view so NaN positions count;
* the key-value metadata.

Every mutation test of the comparator asserts that its own leg fired, not just that some
difference was found.

The inputs are two 200-event NanoAOD v15 fixtures, one MC and one data, each built by a script
checked in beside it. The MC fixture is the first 200 events of a 2024 GluGluH→γγ NanoAODv15 file.
The data fixture has certified and uncertified lumi sections, and its builder puts the diphoton
signal only in events 0–59, so its (100, 200) chunk selects nothing: that part has zero rows and
the original's schema. The suite runs one plan over both fixtures on ``SequentialRunner`` and on
``SubmitRunner(ThreadBackend(2))``: its value must equal ``coffea.processor.accumulate`` of the
original's counters, and each part must match the original's part for its range. It also records every events object coffea hands out while
the plan is built and run, and requires each to be graphed NanoEvents.

``examples/hgg/validate_real.py --parts 2`` runs the same comparison on real 2024 NanoAOD over
xrootd: the first file of ``GluGluHto2G_M-125_amcatnlo_2024`` and the first of ``DataC_2024``. It
prints each file's entry count, every part's ``compare_part`` result, and each dataset's
accumulated counters beside the plan's. It exits 1 on any difference.

The CI job ``test-hgg`` (ubuntu, Python 3.12) installs the coffea fork, ``uproot`` from the commit
the fork needs, and ``higgs_dna`` with ``--no-deps``. It then runs
``GRAPHED_HGG_REQUIRED=1 pytest tests/frozen/m69a``. The main matrix has no coffea, so the files
skip there.
