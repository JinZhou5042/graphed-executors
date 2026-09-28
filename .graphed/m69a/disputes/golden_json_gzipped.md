# Owner ruling: the 2024 golden JSON is committed gzipped, and never uncompressed

**Artifact:** `tests/frozen/m69a/data/Cert_Collisions2024_378981_386951_Golden.json`, 3660 lines of the PR's diff.

**Ruling (owner, 2026-09-27):** the file is committed as `.json.gz`, and the uncompressed `.json` must not appear
anywhere in the branch history. The owner authorized moving the freeze tags this requires.

**Correction:** the branch was rewritten from its first commit, so `data/…Golden.json.gz` has been there from the
start. `hgg_harness.payload(name)` decompresses a gzipped data file whose target is not itself gzipped, and
`place_higgs_dna_data()` writes those bytes where the original reads them. The readers in
`test_hgg_oracle_fixture.py`, `test_hgg_lumimask_plugin.py` and `data/make_hgg_fixture.py` read the same bytes.
Decompressed, the file is byte-identical to the certified JSON (sha256 `32572466413b0b2e…`). `freeze-m69a` and
`freeze-m72` were re-tagged on the rewritten commits.
