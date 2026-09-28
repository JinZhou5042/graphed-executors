# m69a frozen suite — the H→γγ inclusive processor on graphed

This suite freezes the acceptance tests for `examples/hgg/`, the graphed translation of the owner's
coffea processor `inclusive_processor.py` (a distillation of HiggsDNA's H→γγ inclusive processor).
The oracle is the original script itself, byte-identical, run on coffea NanoEvents the way its
`__main__` runs it. The source of truth is `lanes/htcondor/plan-services.md` §4 (the m69a unit) and
constraint C1 of `reviews/plan-services-r5.md`; the m72 re-spec (owner-authorized: one plan over a
fileset, `lanes/multiout/plan-B.md` and `plan-C.md` §2) refroze it. **Frozen — read-only after the
`freeze-m72` tag.** The new API it names (`analysis.plan(fileset, *, year, out)`) fails on the m69a
translation with `TypeError`, and the removal test fails while `HggProcess` exists.

Run it with `GRAPHED_HGG_REQUIRED=1 pytest tests/frozen/m69a` in an environment with the coffea fork
(`graphed-org/coffea-graphed-mvp`), `vector`, `correctionlib` and `higgs_dna` installed `--no-deps`;
the `test-hgg` CI job provides them. Without `GRAPHED_HGG_REQUIRED=1` every file `importorskip`s
coffea and higgs_dna, so the main matrix collects the directory and skips it.

## Files

| File | Plan item | What it shows | A wrong implementation it fails |
|---|---|---|---|
| `data/make_hgg_fixture.py` | §4 fixtures; m72 owner ruling | `--slice 0 200 SRC nano_hgg_v15.root` cuts the MC fixture: entries 0–200, all 2007 branches, of the internal CMS file named in its docstring (`/store/mc/RunIII2024Summer24NanoAODv15/GluGluH-Hto2G_…/acebfb52-….root`, sha256 `019a22e8…`), nothing injected. Without `--slice` it injects two photons and the mainAnalysis triggers into events 0–59 of `nano_tt_v15.root` (seed 20260925); `--data` then drops `GenPart_*`/`GenVtx_*`/`LHE*`/`genWeight` and sets run/lumi from the 2024 golden JSON (events 30–59 uncertified), which is how `nano_hgg_v15_data.root` is built. Fixed clock, file UUID, no compression, and the destination is removed first, so a rebuild under the same basename is byte-identical | — |
| `data/nano_tt_v15.root` | §4 fixtures | The injection mode's input, coffea's `tests/samples/nano_tt_v15.root` (sha256 pinned) | — |
| `data/nano_hgg_v15.root` | m72 owner ruling | The MC fixture: the 200-event GluGluH slice (sha256 pinned; 30 negative `genWeight`s) | — |
| `data/nano_hgg_v15_data.root` | §4 fixtures | The data fixture: injected `nano_tt_v15.root` with golden-JSON run/lumi | — |
| `data/inclusive_processor.py` | §4 oracle | The owner's original, byte-identical (sha256 `791229c2…d1dd`), excluded from ruff and mypy | — |
| `data/Cert_Collisions2024_378981_386951_Golden.json.gz`, `data/jetid.json.gz` | §4 oracle | The 2024 golden JSON (gzipped, placed decompressed) and the 2024 jet-ID set, sha256-equal to what `pull_files.py` places at LPC | — |
| `hgg_harness.py` | §4 harness | `place_higgs_dna_data()`, `payload(name)`, `oracle_parts(uri, dataset, year, ranges, out)`, `compare_part(expected, actual)`; puts `examples/hgg` on `sys.path` | — |
| `conftest.py` | §4 harness | The session fixture that places the two JSONs in the installed `higgs_dna` | — |
| `test_hgg_oracle_fixture.py` | §4 row 1 | The original's sha256; its `infer_nano_version` is the installed `higgs_dna`'s; the placed files equal `data/`'s and are the paths the original resolves; placing over a different file fails naming it; the committed MC fixture has the pinned sha256, 200 entries and 2007 branches; the oracle on it in one chunk gives `nNeg` 30 of 200, an 86 × 145 part, sorted columns, both KV strings and `<file UUID>_Events_0-200.parquet`; on the data fixture the only rows are the rows from events 0–29 (23 rows) of the injected MC the builder writes into `tmp_path`, and every data branch except `run`/`luminosityBlock` equals that MC's (`equal_nan=True`), so the lumi mask is the only difference; the data fixture's (100, 200) chunk is an empty selection; a rebuilt fixture is byte-identical (the MC leg needs `GRAPHED_HGG_MC_SOURCE`, see below) | the wrong original, a fixture drifted from its builder, a data fixture that differs from its MC in more than run/lumi |
| `test_hgg_compare.py` | §4 row 2, C1 | `compare_part` is `[]` for an oracle part against itself re-read from disk, and each mutation fires its own leg, asserted by leg set and entry: one ulp in `PTJ0` (float64; `mass` is float32 in real NanoAOD v15) → `values`; a dropped row → `validity` and `values` in every column; two columns swapped → `schema`; `NJ` int64→int32 with equal values → `schema` (and `values`); one `NJ` value null → `validity` (and `values`); one KV value → `metadata`; `nTot` off by one, or `nTot` as a float → `counters` | a comparator missing any leg (each leg's deletion fails exactly its tests) |
| `test_hgg_conversion.py` | §4 row 3; m72 plan-C §2 | ONE plan over the fileset `{MC, DataC_2024}` at steps (0, 100), (100, 200), on `SequentialRunner` and `SubmitRunner(ThreadBackend(2))`: its value is `coffea.processor.accumulate` of the oracle's four counters, keyed by dataset (not part) with the original's Python types; exactly the four parts `out/<dataset>/nominal/<stem>_Events_<s>-<e>.parquet` exist, and each `compare_part`s `[]` against the oracle's part for its range with the oracle's counters on both sides (so the metadata leg carries the per-part sums; on the data fixture the second part is empty); each dataset's plan `SubmitRunner.submit`-ted on its own, values unioned, equals the one-plan value with byte-identical parts; a file without `steps`, alone or beside a stepped dataset, raises `ValueError` "steps" at build with nothing written; `HggProcess`/`totals`/`_values`/`_union` are gone. Every events object `NanoEventsFactory` hands out while the plan is built and run (both runners) is a `GraphedNanoArray` whose `Photon` has `metric_table`/`delta_r` (an absent name is `False`), both datasets appear in their `metadata["dataset"]`; its source node is `_GraphedTTreeSource`, as a raw `uproot.graphed` array's is, which has no `Photon`; no `examples/hgg` Python file matches `import uproot`/`uproot.`, while `data/make_hgg_fixture.py` does (control) | wrong-row-space counters, a float changed by a rewrite, per-part instead of per-dataset values, a part named without its range or by a blind `0-0` range, whole-file or dataset-wide metadata, a raise on the empty chunk, a collate that differs from the per-dataset runs, a late steps check, events outside NanoEvents, eager NanoEvents built in a task, hand-read uproot arrays |
| `test_hgg_lumimask_plugin.py` | §4 row 4 | `lumi_mask` on (run, lumi) pairs at the edges of certified ranges of three runs, an absent run, and the data fixture's pairs equals coffea's `LumiMask` on `data/`'s golden JSON; its External node's descriptor carries `sha256_bytes` of that JSON | the wrong golden JSON, a mask that differs from coffea's |

## The contract these tests pin

Beyond the names in plan §4 and m72 plan-C §2, the tests rely on these readings of them:

- `analysis.plan(fileset, *, year, out)` returns ONE plan a runner's `run()` executes. `fileset` is
  `{dataset: files}`, `files` coffea `from_root`'s mapping `{uri: {"object_path": "Events", "steps":
  [[start, stop], ...]}}`; each dataset gets what the oracle gets (`metadata={"dataset": …}`,
  `year={dataset: [year]}`); parts are written at `out/<dataset>/nominal/<file stem>_Events_<s>-<e>.parquet`.
- The plan value is `{dataset: {nTot, nPos, nNeg, nEff, genWeightSum}}` summed over that dataset's
  chunks, as `coffea.processor.accumulate` sums the oracle's counters, with the original's Python types.
- `analysis.lumi_mask(run, lumi, year)` takes graphed arrays in one session and returns the deferred
  boolean mask; its single External node's `content_hash` is
  `graphed.preserve.externals.sha256_bytes` of the golden JSON's bytes.
- The events witness records every events object `NanoEventsFactory.events()` returns while
  `analysis.plan(...)` runs, so the analysis builds its events there, and while the plan runs on
  either runner, so a task that builds eager NanoEvents fails it.
- The search for `import uproot`/`uproot.` covers `examples/hgg/**/*.py`.

## The MC fixture's source is not public

`nano_hgg_v15.root` is a 200-event slice of an internal CMS file (owner ruling, 2026-09-25); the full
file is never committed. `test_a_rebuilt_fixture_is_byte_identical` has an `mc` case only when
`GRAPHED_HGG_MC_SOURCE` points at that file (whose sha256 it checks before rebuilding); without it
only the `data` case is collected, whatever `GRAPHED_HGG_REQUIRED` says. The frozen-suite integrity
scan refuses `pytest.skip`, so the case is absent rather than skipped. CI never has the source, so
the committed file's sha256 pin (`test_the_committed_mc_fixture_is_the_pinned_slice`) is what CI checks.
