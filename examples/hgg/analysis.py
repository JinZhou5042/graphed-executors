"""Inclusive H -> gamma gamma processor on graphed (a translation of the owner's ``inclusive_processor.py``).

The processor is the original's, method for method, in the spelling a deferred array needs: events are
coffea NanoEvents in ``mode="graphed"``, ``gak`` stands in for ``ak``, ``gak.with_field`` for
assignment, and every place the original forces a value (``int()``, ``len()``, ``.to_numpy()``,
``bool()`` guards) becomes a plan output or disappears.

One plan covers a whole fileset. Each dataset records its own graph (data and MC differ), and
``graphed.collate`` joins them. Each task writes the original's parquet part for its chunk, with
that chunk's sums in the part's metadata, beside the counters that the runner tree-reduces:

    fileset = {"MC": {"mc.root": {"object_path": "Events", "steps": [[0, 100], [100, 200]]}}}
    plan = analysis.plan(fileset, year="2024", out="out")
    SequentialRunner().run(plan).value              # {dataset: counters}, as coffea's Runner accumulates
"""

from __future__ import annotations

import functools
import gzip
import json
import logging
import operator
import os
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any, ClassVar

import correctionlib
import numpy
from coffea import processor
from coffea.lumi_tools import LumiMask
from coffea.nanoevents import NanoAODSchema, NanoEventsFactory
from graphed import Array, aggregate_plan, collate
from graphed.awkward import gak, parquet_write
from graphed.core import Partition
from graphed.core.execution import Plan
from graphed.preserve.externals import ExternalPlugin, record_external, sha256_bytes
from higgs_dna.utils.misc_utils import infer_nano_version

logger = logging.getLogger(__name__)


def golden_json_path(relative: str) -> str:
    """Resolve a golden-JSON file shipped inside the higgs_dna package."""
    return str(resources.files("higgs_dna.metaconditions").joinpath(relative))


def jme_json_path(relative: str) -> str:
    """Resolve a JME correctionlib file shipped inside the higgs_dna package."""
    return str(resources.files("higgs_dna.systematics").joinpath("JSONs/POG/JME/" + relative))


def metaconditions() -> dict[str, Any]:
    """The triggers and MET filters, from the metaconditions JSON the original's ``__main__`` loads."""
    text = resources.files("higgs_dna.metaconditions").joinpath("Era2022_v1.json").read_text()
    loaded: dict[str, Any] = json.loads(text)
    return loaded


# ---------------------------------------------------------------------- #
# The lumi mask as a preservable External: the golden JSON's bytes are the payload, coffea's
# LumiMask evaluates them on each chunk's eager run/lumi.
# ---------------------------------------------------------------------- #


def _load_lumimask(payload: bytes, params: Mapping[str, Any]) -> LumiMask:
    import fsspec  # noqa: PLC0415  (coffea's dependency; LumiMask opens its JSON through fsspec)

    path = f"memory://hgg-lumimask/{params['content_hash'].replace(':', '-')}.json"
    fsspec.filesystem("memory").pipe(path, payload)
    return LumiMask(path)


def _eval_lumimask(mask: LumiMask, params: Mapping[str, Any], inputs: list[Any]) -> Any:
    run, lumi = inputs
    return mask(run, lumi)


def _lumimask_samples() -> list[bytes]:
    return [b'{"1": [[1, 2]]}', b'{"2": [[3, 4]]}']


LUMIMASK_PLUGIN = ExternalPlugin(
    kind="lumimask",
    content_hash=sha256_bytes,
    evaluate=_eval_lumimask,
    samples=_lumimask_samples,
    load=_load_lumimask,
    framework="coffea",
)


def lumi_mask(run: Array, lumi: Array, year: str) -> Array:
    """The certified (run, lumi) pairs of ``year``'s golden JSON, read where the original reads it."""
    for base in ("2016", "2022", "2023", "2024"):
        if year and base in year:
            year = base
            break
    payload = Path(golden_json_path(HggInclusiveProcessor.GOLDEN_JSON[year])).read_bytes()
    mask = record_external(run.session, LUMIMASK_PLUGIN, payload, [run, lumi])
    # an External's recorded form is its first input's (the run numbers); the mask is boolean
    boolean: Array = gak.values_astype(mask, "bool")
    return boolean


# ---------------------------------------------------------------------- #
# Small object-selection helpers (the original's, deferred).
# ---------------------------------------------------------------------- #


def delta_r_mask(first: Any, second: Any, threshold: float) -> Any:
    """Mask of objects in `first` that are >= threshold away from all `second`."""
    mval = first.metric_table(second)
    return gak.all(mval > threshold, axis=-1)


def choose_jet(jets_variable: Any, n: int, fill_value: float) -> Any:
    """Flatten a jagged jet variable to the n-th jet per event, padding with fill_value."""
    leading = jets_variable[gak.local_index(jets_variable) == n]
    leading = gak.pad_none(leading, 1)
    return gak.flatten(gak.fill_none(leading, fill_value))


def add_jetId(jets: Any, nano_version: int, year: str, flattenUnflatten: bool = False) -> Any:
    """(Re)compute the jet ID bitmap, following the official JetID recipe (v13+: the JME JSON)."""
    abs_eta = abs(jets.eta)

    if nano_version < 12:
        return jets.jetId

    if nano_version == 12:
        passJetIdTight = gak.where(
            abs_eta <= 2.7,
            (jets.jetId & (1 << 1)) > 0,
            gak.where(
                (abs_eta > 2.7) & (abs_eta <= 3.0),
                ((jets.jetId & (1 << 1)) > 0) & (jets.neHEF < 0.99),
                ((jets.jetId & (1 << 1)) > 0) & (jets.neEmEF < 0.4),
            ),
        )
        passJetIdTightLepVeto = gak.where(
            abs_eta <= 2.7,
            passJetIdTight & (jets.muEF < 0.8) & (jets.chEmEF < 0.8),
            passJetIdTight,
        )
        return (passJetIdTight * (1 << 1)) | (passJetIdTightLepVeto * (1 << 2))

    if year == "2025":
        year = "2024"  # 2025 PUPPI tune == 2024, reuse the 2024 jet ID
    jerc_json = {
        "2022preEE": jme_json_path("2022_Summer22/jetid.json.gz"),
        "2022postEE": jme_json_path("2022_Summer22EE/jetid.json.gz"),
        "2023preBPix": jme_json_path("2023_Summer23/jetid.json.gz"),
        "2023postBPix": jme_json_path("2023_Summer23BPix/jetid.json.gz"),
        "2024": jme_json_path("2024_Summer24/jetid.json.gz"),
    }
    # the correctionlib plugin parses JSON bytes, so the gzip is opened here
    payload = gzip.decompress(Path(jerc_json[year]).read_bytes())
    cset = correctionlib.CorrectionSet.from_string(payload.decode())

    if flattenUnflatten:
        counts = gak.num(jets)
        jets = gak.flatten(jets, axis=1)

    eval_dict = {
        "eta": jets.eta,
        "chHEF": jets.chHEF,
        "neHEF": jets.neHEF,
        "chEmEF": jets.chEmEF,
        "neEmEF": jets.neEmEF,
        "muEF": jets.muEF,
        "chMultiplicity": jets.chMultiplicity,
        "neMultiplicity": jets.neMultiplicity,
        "multiplicity": jets.chMultiplicity + jets.neMultiplicity,
    }

    def evaluate(name: str) -> Any:
        inputs = [eval_dict[i.name] for i in cset[name].inputs]
        return gak.apply_correction(payload, name, inputs, None, args=[f"${i}" for i in range(len(inputs))])

    idTight_value = evaluate("AK4PUPPI_Tight") * 2
    idTightLepVeto_value = evaluate("AK4PUPPI_TightLeptonVeto") * 4
    id_value = idTight_value + idTightLepVeto_value

    return gak.unflatten(id_value, counts) if flattenUnflatten else id_value


def get_fiducial_flag(events: Any, flavour: str = "Geometric") -> Any:
    """Particle-level fiducial flag from the generator photons ('Classical' or 'Geometric')."""
    if "iso" in events.GenPart.fields:
        sel_pho = (
            (events.GenPart.pdgId == 22)
            & (events.GenPart.status == 1)
            & (events.GenPart.iso * events.GenPart.pt < 10)
        )
        photons = events.GenPart[sel_pho]
        photons = photons[gak.argsort(photons.pt, ascending=False)]
        gen_photons = gak.pad_none(photons, 2)
    else:
        gen_photons = gak.pad_none(events.GenIsolatedPhoton, 2)

    lead_pho = gen_photons[:, 0]
    sublead_pho = gen_photons[:, 1]
    diphoton = lead_pho + sublead_pho

    if flavour == "Geometric":
        lead_mask = numpy.sqrt(lead_pho.pt * sublead_pho.pt) / diphoton.mass > 1 / 3
    elif flavour == "Classical":
        lead_mask = lead_pho.pt / diphoton.mass > 1 / 3
    sublead_mask = sublead_pho.pt / diphoton.mass > 1 / 4

    lead_eta_mask = (numpy.abs(lead_pho.eta) < 1.4442) | (
        (numpy.abs(lead_pho.eta) < 2.5) & (numpy.abs(lead_pho.eta) > 1.566)
    )
    sublead_eta_mask = (numpy.abs(sublead_pho.eta) < 1.4442) | (
        (numpy.abs(sublead_pho.eta) < 2.5) & (numpy.abs(sublead_pho.eta) > 1.566)
    )

    return gak.fill_none(lead_mask & sublead_mask & lead_eta_mask & sublead_eta_mask, False)


def get_higgs_truth_attributes(events: Any) -> tuple[Any, Any]:
    """Truth-level Higgs pT and rapidity from the HTXS inputs."""
    TruthPTH = gak.fill_none(events.HTXS.Higgs_pt, -999.0)
    TruthYH = gak.fill_none(events.HTXS.Higgs_y, -999.0)
    return TruthPTH, TruthYH


class HggInclusiveProcessor:
    """The original's processor; ``process`` returns what a plan writes and counts instead of doing it."""

    # photon preselection cuts
    min_pt_photon = 25.0
    min_pt_lead_photon = 35.0
    min_mvaid = -0.7
    max_hovere = 0.08
    min_full5x5_r9 = 0.8
    max_chad_iso = 20.0
    max_chad_rel_iso = 0.3

    min_full5x5_r9_EB_high_r9 = 0.85
    min_full5x5_r9_EE_high_r9 = 0.9
    min_full5x5_r9_EB_low_r9 = 0.5
    min_full5x5_r9_EE_low_r9 = 0.8
    max_trkSumPtHollowConeDR03_EB_low_r9 = 6.0
    max_trkSumPtHollowConeDR03_EE_low_r9 = 6.0
    max_sieie_EB_low_r9 = 0.015
    max_sieie_EE_low_r9 = 0.035
    max_pho_iso_EB_low_r9 = 4.0
    max_pho_iso_EE_low_r9 = 4.0

    # effective-area constants for the Run3 quadratic photon-isolation correction
    EA1_EB1 = 0.102056
    EA2_EB1 = -0.000398112
    EA1_EB2 = 0.0820317
    EA2_EB2 = -0.000286224
    EA1_EE1 = 0.0564915
    EA2_EE1 = -0.000248591
    EA1_EE2 = 0.0428606
    EA2_EE2 = -0.000171541
    EA1_EE3 = 0.0395282
    EA2_EE3 = -0.000121398
    EA1_EE4 = 0.0369761
    EA2_EE4 = -8.10369e-05
    EA1_EE5 = 0.0369417
    EA2_EE5 = -2.76885e-05

    # muon selection cuts
    muon_pt_threshold = 10
    muon_max_eta = 2.4
    muon_photon_min_dr = 0.2

    # electron selection cuts
    electron_pt_threshold = 15
    electron_max_eta = 2.5
    electron_photon_min_dr = 0.2

    # jet selection cuts
    jet_pho_min_dr = 0.4
    jet_ele_min_dr = 0.4
    jet_muo_min_dr = 0.4
    jet_pt_threshold = 20
    jet_max_eta = 4.7
    clean_jet_pho = True
    clean_jet_ele = True
    clean_jet_muo = True

    # mapping used when flattening the diphoton record into the output columns
    prefixes: ClassVar[dict[str, str]] = {"pho_lead": "lead", "pho_sublead": "sublead"}

    GOLDEN_JSON: ClassVar[dict[str, str]] = {
        "2016": "CAF/certification/Collisions16/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt",
        "2017": "CAF/certification/Collisions17/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt",
        "2018": "CAF/certification/Collisions18/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt",
        "2022": "CAF/certification/Collisions22/Cert_Collisions2022_355100_362760_Golden.json",
        "2023": "CAF/certification/Collisions23/Cert_Collisions2023_366442_370790_Golden.json",
        "2024": "CAF/certification/Collisions24/Cert_Collisions2024_378981_386951_Golden.json",
        "2025": "CAF/certification/Collisions25/Cert_Collisions2025_391658_398860_Golden.json",
    }

    def __init__(
        self,
        metaconditions: Mapping[str, Any],
        *,
        apply_trigger: bool = True,
        trigger_group: str = ".*DoubleEG.*",
        analysis: str = "mainAnalysis",
        year: Mapping[str, list[str]] | None = None,
        fiducialCuts: str = "classical",
        nano_version: int | None = None,
    ) -> None:
        self.meta = metaconditions
        self.apply_trigger = apply_trigger
        self.trigger_group = trigger_group
        self.analysis = analysis
        self.year = year if year is not None else {}
        self.fiducialCuts = fiducialCuts
        self.nano_version = nano_version

    def get_year(self, dataset_name: str) -> str | None:
        """Return the data-taking year string for this dataset (e.g. '2024')."""
        try:
            return self.year[dataset_name][0]
        except (KeyError, IndexError):
            logger.warning(f"[ inclusive ] No year info for dataset {dataset_name}")
            return None

    def resolve_nano_version(self, events: Any) -> int:
        """Figure out the NanoAOD version if it was not given explicitly."""
        if self.nano_version is None:
            self.nano_version = infer_nano_version(events)
            if self.nano_version is None:
                raise ValueError("Unable to infer NanoAOD version; pass it explicitly via --nano-version.")
            logger.info(f"[ inclusive ] Detected NanoAOD version: {self.nano_version}")
        return self.nano_version

    def apply_filters_and_triggers(self, events: Any) -> Any:
        """Apply the recommended MET noise filters and the diphoton HLT paths."""
        met_filters = self.meta["flashggMetFilters"][self.data_kind]
        filtered = functools.reduce(
            operator.and_,
            (events.Flag[metfilter.split("_")[-1]] for metfilter in met_filters),
        )

        triggered = gak.ones_like(filtered)
        if self.apply_trigger:
            triggers = self.meta["TriggerPaths"][self.trigger_group][self.analysis]
            hlt = events.HLT
            trigger_names = []
            for trigger in triggers:
                actual = trigger.replace("HLT_", "").replace("*", "")
                for field in hlt.fields:
                    if field.startswith(actual):
                        trigger_names.append(field)
            triggered = functools.reduce(operator.or_, (hlt[name] for name in trigger_names))

        return events[filtered & triggered]

    @staticmethod
    def remove_ecal_bad_calib(events: Any) -> Any:
        """Reject events affected by the EcalBadCalibCrystal issue (the affected 2022 runs)."""
        run_mask = (events.run >= 362433) & (events.run <= 367144)
        met_cut = events.PuppiMET.pt > 100

        dphi = numpy.abs(events.PuppiMET.phi - events.Jet.phi) % (2 * numpy.pi)
        dphi = gak.where(dphi > numpy.pi, 2 * numpy.pi - dphi, dphi)
        jet_cuts = (
            (events.Jet.pt > 50)
            & ((events.Jet.eta > -0.5) & (events.Jet.eta < -0.1))
            & ((events.Jet.phi > -2.1) & (events.Jet.phi < -1.8))
            & ((events.Jet.neEmEF > 0.9) | (events.Jet.chEmEF > 0.9))
            & (dphi > 2.9)
        )
        events_to_remove = run_mask & met_cut & gak.any(jet_cuts, axis=1)
        return events[~events_to_remove]

    @staticmethod
    def add_zero_photon_mass_and_charge(photons: Any) -> Any:
        """Photons have no mass/charge in NanoAOD; add zeros so vector ops work."""
        photons = gak.with_field(photons, gak.zeros_like(photons.pt), "mass")
        return gak.with_field(photons, gak.zeros_like(photons.pt), "charge")

    @staticmethod
    def add_photon_sc_eta(photons: Any, PV: Any) -> Any:
        """Add the supercluster eta (``ScEta``): stored from NanoAOD v13, else projected from the PV."""
        if "superclusterEta" in photons.fields:
            return gak.with_field(photons, photons.superclusterEta, "ScEta")

        # only NanoAOD < v13 reaches here
        PV_x, PV_y, PV_z = PV.x, PV.y, PV.z

        mask_barrel = photons.isScEtaEB
        mask_endcap = photons.isScEtaEE

        tg_theta_over_2 = numpy.exp(-photons.eta)
        tg_theta_over_2 = gak.where(tg_theta_over_2 == 1.0, 1 - 1e-10, tg_theta_over_2)
        tg_theta = 2 * tg_theta_over_2 / (1 - tg_theta_over_2 * tg_theta_over_2)

        # barrel: project onto a cylinder of radius R = 130 cm; the original fills a zeros_like(PV_x)
        # buffer by masked assignment, so every branch keeps PV_x's dtype and unmatched rows stay 0
        R = 130.0
        zero = gak.zeros_like(PV_x)
        atan = numpy.arctan(PV_y / PV_x)
        angle_x0_y0 = gak.where(
            PV_x > 0,
            atan,
            gak.where(
                PV_x < 0,
                numpy.pi + atan,
                gak.where(
                    (PV_x == 0) & (PV_y >= 0),
                    zero + numpy.pi / 2,
                    gak.where((PV_x == 0) & (PV_y < 0), zero - numpy.pi / 2, zero),
                ),
            ),
        )

        alpha = angle_x0_y0 + (numpy.pi - photons.phi)
        sin_beta = numpy.sqrt(PV_x**2 + PV_y**2) / R * numpy.sin(alpha)
        beta = numpy.abs(numpy.arcsin(sin_beta))
        gamma = numpy.pi / 2 - alpha - beta
        length = numpy.sqrt(
            R**2 + PV_x**2 + PV_y**2 - 2 * R * numpy.sqrt(PV_x**2 + PV_y**2) * numpy.cos(gamma)
        )
        z0_zSC = length / tg_theta
        tg_sctheta = gak.where(mask_barrel, R / (PV_z + z0_zSC), tg_theta)

        # endcap: project onto the endcap plane at |z| = 310 cm
        intersection_z = gak.where(photons.eta > 0, 310.0, -310.0)
        r = (intersection_z - PV_z) * tg_theta
        crystalX = PV_x + r * numpy.cos(photons.phi)
        crystalY = PV_y + r * numpy.sin(photons.phi)
        tg_sctheta = gak.where(
            mask_endcap, numpy.sqrt(crystalX**2 + crystalY**2) / intersection_z, tg_sctheta
        )

        sctheta = numpy.arctan(tg_sctheta)
        sctheta = gak.where(sctheta < 0, numpy.pi + sctheta, sctheta)
        return gak.with_field(photons, -numpy.log(numpy.tan(sctheta / 2)), "ScEta")

    def photon_preselection(self, photons: Any, events: Any, year: str | None) -> Any:
        """HLT-mimicking single-photon preselection, applied per photon."""
        rho = events.Rho.fixedGridRhoAll * gak.ones_like(photons.pt)
        photon_abs_eta = numpy.abs(photons.eta)

        # the |eta| bin edges are strict: a photon exactly on an edge fails every bin
        if year in ["2016", "2016PreVFP", "2016PostVFP", "2017", "2018"]:
            pass_phoIso_EB = photons.pfPhoIso03 - rho * 0.16544 < self.max_pho_iso_EB_low_r9
            pass_phoIso_EE = photons.pfPhoIso03 - rho * 0.13212 < self.max_pho_iso_EE_low_r9
        else:

            def band(lo: float, hi: float, ea1: float, ea2: float, cut: float) -> Any:
                return (
                    (photon_abs_eta > lo)
                    & (photon_abs_eta < hi)
                    & (photons.pfPhoIso03 - rho * ea1 - rho * rho * ea2 < cut)
                )

            eb, ee = self.max_pho_iso_EB_low_r9, self.max_pho_iso_EE_low_r9
            pass_phoIso_EB = band(0.0, 1.0, self.EA1_EB1, self.EA2_EB1, eb) | band(
                1.0, 1.4442, self.EA1_EB2, self.EA2_EB2, eb
            )
            pass_phoIso_EE = (
                band(1.566, 2.0, self.EA1_EE1, self.EA2_EE1, ee)
                | band(2.0, 2.2, self.EA1_EE2, self.EA2_EE2, ee)
                | band(2.2, 2.3, self.EA1_EE3, self.EA2_EE3, ee)
                | band(2.3, 2.4, self.EA1_EE4, self.EA2_EE4, ee)
                | band(2.4, 2.5, self.EA1_EE5, self.EA2_EE5, ee)
            )

        # tracker isolation: branch name changed across NanoAOD versions
        fields = photons.fields
        iso = (
            photons.trkSumPtHollowConeDR03 if "trkSumPtHollowConeDR03" in fields else photons.pfChargedIsoPFPV
        )
        rel_iso = photons.pfRelIso03_chg if "pfRelIso03_chg" in fields else photons.pfRelIso03_chg_quadratic

        isEB_high_r9 = photons.isScEtaEB & (photons.r9 > self.min_full5x5_r9_EB_high_r9)
        isEE_high_r9 = photons.isScEtaEE & (photons.r9 > self.min_full5x5_r9_EE_high_r9)
        isEB_low_r9 = (
            photons.isScEtaEB
            & (photons.r9 > self.min_full5x5_r9_EB_low_r9)
            & (photons.r9 < self.min_full5x5_r9_EB_high_r9)
            & (iso < self.max_trkSumPtHollowConeDR03_EB_low_r9)
            & (photons.sieie < self.max_sieie_EB_low_r9)
            & pass_phoIso_EB
        )
        isEE_low_r9 = (
            photons.isScEtaEE
            & (photons.r9 > self.min_full5x5_r9_EE_low_r9)
            & (photons.r9 < self.min_full5x5_r9_EE_high_r9)
            & (iso < self.max_trkSumPtHollowConeDR03_EE_low_r9)
            & (photons.sieie < self.max_sieie_EE_low_r9)
            & pass_phoIso_EE
        )

        return photons[
            (photons.electronVeto == 1)
            & (photons.pt > self.min_pt_photon)
            & (photons.isScEtaEB | photons.isScEtaEE)
            & (photons.mvaID > self.min_mvaid)
            & (photons.hoe < self.max_hovere)
            & (
                (photons.r9 > self.min_full5x5_r9)
                | (rel_iso * photons.pt < self.max_chad_iso)
                | (rel_iso < self.max_chad_rel_iso)
            )
            & (isEB_high_r9 | isEB_low_r9 | isEE_high_r9 | isEE_low_r9)
        ]

    def select_electrons(self, electrons: Any, diphotons: Any) -> Any:
        """Loose, isolated electrons away from both photons (for jet cleaning)."""
        pt_cut = electrons.pt > self.electron_pt_threshold
        eta_cut = abs(electrons.eta) < self.electron_max_eta
        is_transition = (abs(electrons.eta) > 1.4442) & (abs(electrons.eta) < 1.566)
        eta_cut = eta_cut & (~is_transition)
        id_cut = electrons.cutBased >= 2  # loose
        dr_lead = delta_r_mask(electrons, diphotons.pho_lead, self.electron_photon_min_dr)
        dr_sublead = delta_r_mask(electrons, diphotons.pho_sublead, self.electron_photon_min_dr)
        return pt_cut & eta_cut & id_cut & dr_lead & dr_sublead

    def select_muons(self, muons: Any, diphotons: Any) -> Any:
        """Medium, tight-isolated global muons away from both photons (for jet cleaning)."""
        pt_cut = muons.pt > self.muon_pt_threshold
        eta_cut = abs(muons.eta) < self.muon_max_eta
        id_cut = muons.mediumId
        iso_cut = muons.pfIsoId >= 4
        global_cut = muons.isGlobal
        dr_lead = delta_r_mask(muons, diphotons.pho_lead, self.muon_photon_min_dr)
        dr_sublead = delta_r_mask(muons, diphotons.pho_sublead, self.muon_photon_min_dr)
        return pt_cut & eta_cut & id_cut & iso_cut & global_cut & dr_lead & dr_sublead

    def select_jets(self, jets: Any, diphotons: Any, muons: Any, electrons: Any) -> Any:
        """tightLepVeto jets passing pT/eta and cleaned against photons/leptons.

        The original guards each cleaning on a non-empty chunk; the deferred ops are total on an
        empty one, so the guards are gone and an empty chunk gives the same empty masks."""
        jetId_cut = jets.jetId == 6  # tightLepVeto
        pt_cut = jets.pt > self.jet_pt_threshold
        eta_cut = abs(jets.eta) < self.jet_max_eta

        if self.clean_jet_pho:
            kinematics = ("pt", "eta", "phi", "mass", "charge")
            lead = gak.with_name(
                gak.zip({k: diphotons.pho_lead[k] for k in kinematics}), "PtEtaPhiMCandidate"
            )
            sublead = gak.with_name(
                gak.zip({k: diphotons.pho_sublead[k] for k in kinematics}), "PtEtaPhiMCandidate"
            )
            dr_pho_lead = delta_r_mask(jets, lead, self.jet_pho_min_dr)
            dr_pho_sublead = delta_r_mask(jets, sublead, self.jet_pho_min_dr)
        else:
            dr_pho_lead = jets.pt > -1
            dr_pho_sublead = jets.pt > -1

        dr_ele = delta_r_mask(jets, electrons, self.jet_ele_min_dr) if self.clean_jet_ele else jets.pt > -1
        dr_muo = delta_r_mask(jets, muons, self.jet_muo_min_dr) if self.clean_jet_muo else jets.pt > -1

        return jetId_cut & pt_cut & eta_cut & dr_pho_lead & dr_pho_sublead & dr_ele & dr_muo

    def add_jet_variables(self, diphotons: Any, events: Any, year: str) -> Any:
        """Cleaned, pT-ordered jets; attach the jet-counting / leading-jet columns to the diphotons."""
        nano_version = self.resolve_nano_version(events)
        raw = events.Jet
        jets = gak.zip(
            {
                "pt": raw.pt,
                "eta": raw.eta,
                "phi": raw.phi,
                "mass": raw.mass,
                "charge": gak.zeros_like(raw.pt),
                "jetId": add_jetId(raw, nano_version, year, flattenUnflatten=True),
                **(
                    {"neHEF": raw.neHEF, "neEmEF": raw.neEmEF, "chEmEF": raw.chEmEF, "muEF": raw.muEF}
                    if nano_version == 12
                    else {}
                ),
                **(
                    {
                        "neHEF": raw.neHEF,
                        "neEmEF": raw.neEmEF,
                        "chMultiplicity": raw.chMultiplicity,
                        "neMultiplicity": raw.neMultiplicity,
                        "chEmEF": raw.chEmEF,
                        "chHEF": raw.chHEF,
                        "muEF": raw.muEF,
                    }
                    if nano_version >= 13
                    else {}
                ),
            }
        )
        jets = gak.with_name(jets, "PtEtaPhiMCandidate")

        e = events.Electron
        electrons = gak.with_name(
            gak.zip(
                {
                    "pt": e.pt,
                    "eta": e.eta,
                    "phi": e.phi,
                    "mass": e.mass,
                    "charge": e.charge,
                    "cutBased": e.cutBased,
                    "mvaIso_WP90": e.mvaIso_WP90,
                    "mvaIso_WP80": e.mvaIso_WP80,
                }
            ),
            "PtEtaPhiMCandidate",
        )

        # the base processor's extra iso cut on muons
        m = events.Muon[events.Muon.pfRelIso03_all < 0.2]
        muons = gak.with_name(
            gak.zip(
                {
                    "pt": m.pt,
                    "eta": m.eta,
                    "phi": m.phi,
                    "mass": m.mass,
                    "charge": m.charge,
                    "tightId": m.tightId,
                    "mediumId": m.mediumId,
                    "looseId": m.looseId,
                    "isGlobal": m.isGlobal,
                    "pfIsoId": m.pfIsoId,
                }
            ),
            "PtEtaPhiMCandidate",
        )

        sel_electrons = electrons[self.select_electrons(electrons, diphotons)]
        sel_muons = muons[self.select_muons(muons, diphotons)]

        jets = jets[self.select_jets(jets, diphotons, sel_muons, sel_electrons)]
        jets = jets[gak.argsort(jets.pt, ascending=False)]

        first_jet_pt = choose_jet(jets.pt, 0, -999.0)
        first_jet_eta = choose_jet(jets.eta, 0, -999.0)
        first_jet_pz = first_jet_pt * numpy.sinh(first_jet_eta)
        first_jet_energy = numpy.sqrt(
            (first_jet_pt**2 * numpy.cosh(first_jet_eta) ** 2) + choose_jet(jets.mass, 0, -999.0) ** 2
        )
        first_jet_y = 0.5 * numpy.log((first_jet_energy + first_jet_pz) / (first_jet_energy - first_jet_pz))
        first_jet_y = gak.fill_none(first_jet_y, -999)
        first_jet_y = gak.where(numpy.isnan(first_jet_y), -999, first_jet_y)

        columns = {
            "n_jets": gak.num(jets),
            "NJ": gak.num(jets[(jets.pt > 30) & (numpy.abs(jets.eta) < 2.5)]),
            "PTJ0": first_jet_pt,
            "first_jet_eta": first_jet_eta,
            "first_jet_phi": choose_jet(jets.phi, 0, -999.0),
            "first_jet_mass": choose_jet(jets.mass, 0, -999.0),
            "first_jet_charge": choose_jet(jets.charge, 0, -999.0),
            "PTJ1": choose_jet(jets.pt, 1, -999.0),
            "second_jet_eta": choose_jet(jets.eta, 1, -999.0),
            "second_jet_phi": choose_jet(jets.phi, 1, -999.0),
            "second_jet_mass": choose_jet(jets.mass, 1, -999.0),
            "second_jet_charge": choose_jet(jets.charge, 1, -999.0),
            "YJ0": first_jet_y,
        }
        for name, column in columns.items():
            diphotons = gak.with_field(diphotons, column, name)
        return diphotons

    def build_diphoton_candidates(self, photons: Any) -> Any:
        """Pair up the preselected photons and compute the diphoton kinematics."""
        sorted_photons = photons[gak.argsort(photons.pt, ascending=False)]
        diphotons = gak.combinations(sorted_photons, 2, fields=["pho_lead", "pho_sublead"])

        diphotons = diphotons[diphotons["pho_lead"].pt > self.min_pt_lead_photon]

        diphoton_4mom = diphotons["pho_lead"] + diphotons["pho_sublead"]
        columns = {
            "pt": diphoton_4mom.pt,
            "eta": diphoton_4mom.eta,
            "phi": diphoton_4mom.phi,
            "mass": diphoton_4mom.mass,
            "charge": diphoton_4mom.charge,
            "rapidity": 0.5
            * numpy.log((diphoton_4mom.energy + diphoton_4mom.z) / (diphoton_4mom.energy - diphoton_4mom.z)),
        }
        for name, column in columns.items():
            diphotons = gak.with_field(diphotons, column, name)

        diphotons = diphotons[gak.argsort(diphotons.pt, ascending=False)]
        return gak.with_name(diphotons, "PtEtaPhiMCandidate")

    def apply_fiducial_cut_det_level(self, diphotons: Any) -> Any:
        """Detector-level fiducial selection (classical or geometric)."""
        lead = diphotons.pho_lead
        sublead = diphotons.pho_sublead
        rel = "pfRelIso03_all" if "pfRelIso03_all" in lead.fields else "pfRelIso03_all_quadratic"
        lead_rel_iso, sublead_rel_iso = lead[rel], sublead[rel]

        common = (
            (sublead.pt / diphotons.mass > 1 / 4)
            & (lead_rel_iso * lead.pt < 10)
            & (sublead_rel_iso * sublead.pt < 10)
            & (numpy.abs(lead.eta) < 2.5)
            & (numpy.abs(sublead.eta) < 2.5)
        )
        if self.fiducialCuts == "classical":
            passed = (lead.pt / diphotons.mass > 1 / 3) & common
        elif self.fiducialCuts == "geometric":
            passed = (numpy.sqrt(lead.pt * sublead.pt) / diphotons.mass > 1 / 3) & common
        elif self.fiducialCuts == "none":
            passed = lead.pt > -1
        else:
            raise ValueError(f"Unsupported fiducialCuts mode: {self.fiducialCuts}")

        return diphotons[passed]

    @staticmethod
    def compute_sigma_m_over_m(diphotons: Any) -> Any:
        """Relative diphoton mass resolution from the per-photon energy errors."""
        lead, sublead = diphotons.pho_lead, diphotons.pho_sublead
        lead_e = lead.pt * numpy.cosh(lead.eta)
        sublead_e = sublead.pt * numpy.cosh(sublead.eta)
        value = 0.5 * numpy.sqrt((lead.energyErr / lead_e) ** 2 + (sublead.energyErr / sublead_e) ** 2)
        return gak.with_field(diphotons, value, "sigma_m_over_m")

    def diphoton_to_ak_array(self, diphotons: Any) -> Any:
        """The flat output columns: photon legs prefixed ``lead_``/``sublead_``, the rest as-is,
        without the per-photon copy of the event-level rho, in the sorted order the original writes."""
        output = {}
        for field in gak.fields(diphotons):
            prefix = self.prefixes.get(field, "")
            if prefix:
                for subfield in gak.fields(diphotons[field]):
                    if subfield != "__systematics__":
                        output[f"{prefix}_{subfield}"] = diphotons[field][subfield]
            else:
                output[field] = diphotons[field]
        return gak.zip({k: output[k] for k in sorted(output) if "lead_fixedGridRhoAll" not in k})

    def process(self, events: Any) -> dict[str, Any]:
        """The flat diphoton record, the counters, and the part's key-value metadata (its arrays
        are this chunk's sums)."""
        self.resolve_nano_version(events)
        dataset_name = events.metadata["dataset"]
        self.data_kind = "mc" if "GenPart" in events.fields else "data"
        year = self.get_year(dataset_name)

        # bookkeeping before any selection; Counters gives each the original's Python type
        if self.data_kind == "mc":
            counters = {
                "nTot": gak.num(events.genWeight, axis=0),
                "nPos": gak.sum(events.genWeight > 0),
                "nNeg": gak.sum(events.genWeight < 0),
                "genWeightSum": gak.sum(events.genWeight),
            }
        else:
            counters = {"nTot": gak.num(events, axis=0)}

        if self.data_kind == "data" and year is not None:
            events = events[lumi_mask(events.run, events.luminosityBlock, year)]

        metadata: dict[str, Any] = {"sum_genw_presel": "Data"}
        if self.data_kind == "mc":
            metadata["sum_genw_presel"] = gak.sum(events.genWeight)

        events = self.apply_filters_and_triggers(events)

        if self.data_kind == "data" and year not in ["2018", "2017", "2016preVFP", "2016postVFP"]:
            events = self.remove_ecal_bad_calib(events)

        photons = self.add_zero_photon_mass_and_charge(events.Photon)
        events = gak.with_field(events, self.add_photon_sc_eta(photons, events.PV), "Photon")
        photons = self.photon_preselection(events.Photon, events, year)

        diphotons = self.build_diphoton_candidates(photons)
        diphotons = self.apply_fiducial_cut_det_level(diphotons)
        diphotons = self.add_jet_variables(diphotons, events, year or "")

        diphotons = gak.firsts(diphotons)
        selection_mask = ~gak.is_none(diphotons)
        diphotons = diphotons[selection_mask]
        sel_events = events[selection_mask]

        columns = {
            "event": sel_events.event,
            "lumi": sel_events.luminosityBlock,
            "run": sel_events.run,
            "nPV": sel_events.PV.npvs,
            "fixedGridRhoAll": sel_events.Rho.fixedGridRhoAll,
            "BeamSpot_sigmaZ": sel_events.BeamSpot.sigmaZ,
            "BeamSpot_sigmaZError": sel_events.BeamSpot.sigmaZError,
        }
        if self.data_kind == "mc":
            TruthPTH, TruthYH = get_higgs_truth_attributes(sel_events)
            columns |= {
                "genWeight": sel_events.genWeight,
                "dZ": sel_events.GenVtx.z - sel_events.PV.z,
                "weight": sel_events.genWeight,
                "weight_central": gak.ones_like(sel_events.genWeight),
                "fiducialClassicalFlag": get_fiducial_flag(sel_events, flavour="Classical"),
                "fiducialGeometricFlag": get_fiducial_flag(sel_events, flavour="Geometric"),
                "TruthPTH": TruthPTH,
                "TruthYH": TruthYH,
            }
            metadata["sum_weight_central"] = gak.sum(sel_events.genWeight)
        else:
            columns |= {
                "dZ": gak.zeros_like(sel_events.PV.z),
                "weight": gak.ones_like(sel_events.event),
                "weight_central": gak.ones_like(sel_events.event),
            }
        for name, column in columns.items():
            diphotons = gak.with_field(diphotons, column, name)

        diphotons = self.compute_sigma_m_over_m(diphotons)
        return {"record": self.diphoton_to_ak_array(diphotons), "counters": counters, "metadata": metadata}


# ---------------------------------------------------------------------- #
# The plan: every dataset's graph in one plan; each task writes its part and returns its counters.
# ---------------------------------------------------------------------- #


@dataclass(frozen=True)
class Counters:
    """A chunk's counters as the original returns them, from the plan's values (paths follow them)."""

    names: tuple[str, ...]

    def __call__(self, values: list[Any]) -> dict[str, Any]:
        v = dict(zip(self.names, values, strict=False))
        n = int(v["nTot"])
        nPos, nNeg = int(v.get("nPos", n)), int(v.get("nNeg", 0))
        return {
            "nTot": n,
            "nPos": nPos,
            "nNeg": nNeg,
            "nEff": nPos - nNeg,
            "genWeightSum": float(v.get("genWeightSum", n)),
        }


def accumulate(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    """Two chunks' counters summed, as coffea's Runner accumulates the original's returns."""
    return processor.accumulate([a, b])  # type: ignore[no-any-return]


def part_name(partition: Partition) -> str:
    """The original's part name (``<file>_<tree>_<start>-<stop>``), with the file's stem for its UUID."""
    if partition.is_blind:
        raise ValueError(
            f"{partition.uri} has no entry range to name its part from: give every file explicit "
            '"steps" in the fileset (the counters depend on the chunking too)'
        )
    tree = partition.tree.strip("/").split(";")[0]
    return f"{Path(partition.uri).stem}_{tree}_{partition.entry_start}-{partition.entry_stop}.parquet"


def dataset_plan(dataset: str, files: Mapping[str, Any], *, year: str, out: str) -> Plan[dict[str, Any]]:
    """One dataset's plan over coffea's ``files`` mapping, whose files each give explicit "steps"."""
    events = NanoEventsFactory.from_root(
        dict(files), schemaclass=NanoAODSchema, mode="graphed", metadata={"dataset": dataset}
    ).events()
    outputs = HggInclusiveProcessor(metaconditions(), year={dataset: [year]}).process(events)
    counters: dict[str, Array] = outputs["counters"]
    part = parquet_write(
        outputs["record"],
        os.path.join(out, dataset, "nominal"),
        name=part_name,
        metadata=outputs["metadata"],
        arrow_options={"extensionarray": False},
    )
    return aggregate_plan(
        *counters.values(),
        reduce=Counters(tuple(counters)),
        combine=accumulate,
        empty=dict,
        writes=[part],
    )


def plan(fileset: Mapping[str, Mapping[str, Any]], *, year: str, out: str) -> Plan[dict[str, Any]]:
    """One plan over coffea's ``{dataset: files}``: ``run(plan).value`` is ``{dataset: counters}``,
    and each chunk's part lands at ``out/<dataset>/nominal/<file stem>_Events_<start>-<stop>.parquet``."""
    return collate({ds: dataset_plan(ds, files, year=year, out=out) for ds, files in fileset.items()})


__all__ = [
    "LUMIMASK_PLUGIN",
    "Counters",
    "HggInclusiveProcessor",
    "dataset_plan",
    "lumi_mask",
    "part_name",
    "plan",
]
