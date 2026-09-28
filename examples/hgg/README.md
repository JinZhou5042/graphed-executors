# H→γγ inclusive processor on graphed

`analysis.py` is a translation of `inclusive_processor.py`, a standalone distillation of HiggsDNA's
H→γγ inclusive base processor, to graphed. It reads coffea NanoEvents in `mode="graphed"` with
`NanoAODSchema`, applies the same selections, and writes the same flat diphoton parquet, one part
per chunk, with the original's columns and key-value metadata. One plan covers a whole fileset, MC
and data together, and its value is the original's counters summed per dataset
`{dataset: {nTot, nPos, nNeg, nEff, genWeightSum}}`, as coffea's Runner accumulates them.

| File | What it is |
|---|---|
| `analysis.py` | the processor, `plan(fileset, *, year, out)`, `dataset_plan(dataset, files, *, year, out)`, `lumi_mask(run, lumi, year)` |
| `run_local.py` | run files of one dataset on this machine, in-process or on a thread pool, and print the counters |
| `validate_real.py` | run the original on ranges of a real 2024 MC file and a real 2024 data file, and one graphed plan over both, and compare every part and both datasets' counters |

## Running it

You need the coffea fork with graphed mode (`graphed-org/coffea-graphed-mvp`), `correctionlib`,
`pyarrow`, and `higgs_dna` installed with `--no-deps`. The processor reads HiggsDNA's data files
where the original reads them, inside the installed `higgs_dna` package. HiggsDNA's
`pull_files.py --target GoldenJSON` and `--target JetMET` put them there, or the m69a test harness
copies the 2024 golden JSON and jet-ID set in.

```bash
python examples/hgg/run_local.py FILE.root [FILE.root ...] --dataset MC --year 2024 --parts 4 --workers 4 --out output_inclusive
```

```python
from graphed.core import SequentialRunner
import analysis

steps = [[0, 50_000], [50_000, 100_000]]
fileset = {
    "MC": {mc_uri: {"object_path": "Events", "steps": steps}},
    "DataC_2024": {data_uri: {"object_path": "Events", "steps": steps}},
}
plan = analysis.plan(fileset, year="2024", out="out")
value = SequentialRunner().run(plan).value   # {"MC": {...}, "DataC_2024": {...}}
```

Data and MC record different graphs; `graphed.collate` runs both in the one plan. `plan` over one
dataset, `plan({dataset: files})`, also runs on its own, and a dict union of those values is the
same result.

Each part goes to `out/<dataset>/nominal/<file stem>_Events_<start>-<stop>.parquet`. That is the
original's name, except that the original begins it with the file's UUID and this begins it with
the file name. Every file needs explicit `steps`: a part is named from its range, and the counters
depend on the chunking.

## How it is checked

The frozen suite `tests/frozen/m69a` imports the original script, byte-identical, and runs it as
the oracle on the same ranges. It then compares every graphed part with the original's part: the
arrow schema, each column's validity bitmap and valid values bit for bit, and the key-value
metadata; the plan's value must equal the original's counters accumulated, with their Python
types. The inputs are two 200-event NanoAOD v15 fixtures: the first 200 events of a 2024
GluGluH→γγ MC file, and a data file with certified and uncertified lumi sections.

`validate_real.py --parts 2` does the same on the first file of `GluGluHto2G_M-125_amcatnlo_2024`
and of `DataC_2024` over xrootd. It exits 1 on any difference.

`docs/hgg.rst` lists the eleven places the translation departs from the original's spelling.
