"""
Inclusive H -> gamma gamma processor (standalone, teaching version).

This is a deliberately *simplified*, self-contained processor for an inclusive
H -> gamma gamma cross section measurement. Everything needed to go from a
NanoAOD ``events`` array to a flat parquet file of diphoton candidates lives in
this single file: luminosity masking, MET filters + triggers, the photon
preselection, the diphoton candidate building and the detector-level fiducial
selection.

It reproduces, column for column, what the full ``higgs_dna`` base processor
writes out when run *without* any corrections or systematics. It intentionally
drops only the things that depend on those (or are otherwise out of scope):
  * object/weight corrections and systematics (no scale & smearing, no SFs, ...)
  * b-tagging / heavy-flavour variables
  * taggers / event categorisation
  * photon-ID and diphoton MVA recomputation, CQR
  * normalising-flow corrections and mass-resolution decorrelation

so that the *structure* of the analysis (selections + how events are built and
written out) is visible in one place. The basic objects are all here: photon
preselection, diphoton building, the detector-level fiducial cut, the cleaned
jet-counting / leading-jet variables, and the particle-level truth + fiducial
flags. The only things it borrows from higgs_dna are data files: the
metaconditions JSON (triggers / MET filters), the golden-JSON lumi files and the
JME jet-ID JSON, all read from the installed package.

The event weight is simply the generator weight for MC and 1 for data.

Just run it directly, no arguments:

    python inclusive_processor.py

It reads the whole GluGluH_Hto2G_2024v15.root NanoAOD file, applies the basic
preselection and writes the diphoton parquet under output_inclusive/.
"""

import os
import json
import functools
import operator
import logging
from importlib import resources

import numpy
import awkward as ak
import vector
import correctionlib

from coffea import processor
from coffea.lumi_tools import LumiMask

from higgs_dna.utils.misc_utils import infer_nano_version

logger = logging.getLogger(__name__)

vector.register_awkward()


def golden_json_path(relative: str) -> str:
    """Resolve a golden-JSON file shipped inside the higgs_dna package."""
    return str(resources.files("higgs_dna.metaconditions").joinpath(relative))


def jme_json_path(relative: str) -> str:
    """Resolve a JME correctionlib file shipped inside the higgs_dna package."""
    return str(resources.files("higgs_dna.systematics").joinpath("JSONs/POG/JME/" + relative))


# ---------------------------------------------------------------------- #
# Small object-selection helpers (inlined from higgs_dna so this file is
# self-contained). These are pure geometry/bookkeeping, not corrections.
# ---------------------------------------------------------------------- #

def delta_r_mask(first: ak.Array, second: ak.Array, threshold: float) -> ak.Array:
    """Mask of objects in `first` that are >= threshold away from all `second`."""
    mval = first.metric_table(second)
    return ak.all(mval > threshold, axis=-1)


def choose_jet(jets_variable: ak.Array, n: int, fill_value: float) -> ak.Array:
    """Flatten a jagged jet variable to the n-th jet per event, padding with fill_value."""
    leading = jets_variable[ak.local_index(jets_variable) == n]
    leading = ak.pad_none(leading, 1)
    return ak.flatten(ak.fill_none(leading, fill_value))


def add_jetId(jets: ak.Array, nano_version: int, year: str, flattenUnflatten: bool = False) -> ak.Array:
    """
    (Re)compute the jet ID bitmap, following the official JetID recipe. For
    NanoAOD v13+ this is read from the JME correctionlib JSON (a definition of
    the ID, not a physics correction). Inlined from higgs_dna.tools.jetID.
    """
    abs_eta = abs(jets.eta)

    if nano_version < 12:
        return jets.jetId

    if nano_version == 12:
        passJetIdTight = ak.where(
            abs_eta <= 2.7,
            (jets.jetId & (1 << 1)) > 0,
            ak.where(
                (abs_eta > 2.7) & (abs_eta <= 3.0),
                ((jets.jetId & (1 << 1)) > 0) & (jets.neHEF < 0.99),
                ((jets.jetId & (1 << 1)) > 0) & (jets.neEmEF < 0.4),
            ),
        )
        passJetIdTightLepVeto = ak.where(
            abs_eta <= 2.7,
            passJetIdTight & (jets.muEF < 0.8) & (jets.chEmEF < 0.8),
            passJetIdTight,
        )
        return (passJetIdTight * (1 << 1)) | (passJetIdTightLepVeto * (1 << 2))

    # NanoAOD v13+: read the ID definition from correctionlib
    if year == "2025":
        year = "2024"  # 2025 PUPPI tune == 2024, reuse the 2024 jet ID
    jerc_json = {
        "2022preEE": jme_json_path("2022_Summer22/jetid.json.gz"),
        "2022postEE": jme_json_path("2022_Summer22EE/jetid.json.gz"),
        "2023preBPix": jme_json_path("2023_Summer23/jetid.json.gz"),
        "2023postBPix": jme_json_path("2023_Summer23BPix/jetid.json.gz"),
        "2024": jme_json_path("2024_Summer24/jetid.json.gz"),
    }
    cset = correctionlib.CorrectionSet.from_file(jerc_json[year])

    if flattenUnflatten:
        counts = ak.num(jets)
        jets = ak.flatten(jets, axis=1)

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
    idTight = cset["AK4PUPPI_Tight"]
    idTight_value = idTight.evaluate(*[eval_dict[i.name] for i in idTight.inputs]) * 2
    idTightLepVeto = cset["AK4PUPPI_TightLeptonVeto"]
    idTightLepVeto_value = idTightLepVeto.evaluate(*[eval_dict[i.name] for i in idTightLepVeto.inputs]) * 4
    id_value = idTight_value + idTightLepVeto_value

    return ak.unflatten(id_value, counts) if flattenUnflatten else id_value


def get_fiducial_flag(events: ak.Array, flavour: str = "Geometric") -> ak.Array:
    """
    Particle-level fiducial flag from the generator photons. 'Classical' uses
    the leading-photon pT/mass cut, 'Geometric' the geometric-mean variant.
    Inlined from higgs_dna.tools.gen_helpers.
    """
    if "iso" in events.GenPart.fields:
        sel_pho = (events.GenPart.pdgId == 22) & (events.GenPart.status == 1) & (events.GenPart.iso * events.GenPart.pt < 10)
        photons = events.GenPart[sel_pho]
        photons = photons[ak.argsort(photons.pt, ascending=False)]
        gen_photons = ak.pad_none(photons, 2)
    else:
        gen_photons = ak.pad_none(events.GenIsolatedPhoton, 2)

    lead_pho = gen_photons[:, 0]
    sublead_pho = gen_photons[:, 1]
    diphoton = lead_pho + sublead_pho

    if flavour == "Geometric":
        lead_mask = numpy.sqrt(lead_pho.pt * sublead_pho.pt) / diphoton.mass > 1 / 3
    elif flavour == "Classical":
        lead_mask = lead_pho.pt / diphoton.mass > 1 / 3
    sublead_mask = sublead_pho.pt / diphoton.mass > 1 / 4

    lead_eta_mask = (numpy.abs(lead_pho.eta) < 1.4442) | ((numpy.abs(lead_pho.eta) < 2.5) & (numpy.abs(lead_pho.eta) > 1.566))
    sublead_eta_mask = (numpy.abs(sublead_pho.eta) < 1.4442) | ((numpy.abs(sublead_pho.eta) < 2.5) & (numpy.abs(sublead_pho.eta) > 1.566))

    return ak.fill_none(lead_mask & sublead_mask & lead_eta_mask & sublead_eta_mask, False)


def get_higgs_truth_attributes(events: ak.Array):
    """Truth-level Higgs pT and rapidity from the HTXS inputs."""
    TruthPTH = ak.fill_none(events.HTXS.Higgs_pt, -999.0)
    TruthYH = ak.fill_none(events.HTXS.Higgs_y, -999.0)
    return TruthPTH, TruthYH


class HggInclusiveProcessor(processor.ProcessorABC):  # type: ignore
    # ------------------------------------------------------------------ #
    # Selection cut values (class-level so they are easy to find/tweak).
    # ------------------------------------------------------------------ #

    # photon preselection cuts
    min_pt_photon = 25.0          # min pT for any preselected photon
    min_pt_lead_photon = 35.0     # min pT for the leading photon of a pair
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
    mu_id_wp = "medium"
    mu_iso_wp = "tight"
    muon_photon_min_dr = 0.2
    global_muon = True
    muon_max_dxy = None
    muon_max_dz = None

    # electron selection cuts
    electron_pt_threshold = 15
    electron_max_eta = 2.5
    electron_photon_min_dr = 0.2
    el_id_wp = "loose"  # this includes isolation
    electron_max_dxy = None
    electron_max_dz = None

    # jet selection cuts
    jet_jetId = "tightLepVeto"
    jet_dipho_min_dr = 0.4
    jet_pho_min_dr = 0.4
    jet_ele_min_dr = 0.4
    jet_muo_min_dr = 0.4
    jet_pt_threshold = 20
    jet_max_eta = 4.7
    clean_jet_dipho = False
    clean_jet_pho = True
    clean_jet_ele = True
    clean_jet_muo = True

    # mapping used when flattening the diphoton record into the output columns
    prefixes = {"pho_lead": "lead", "pho_sublead": "sublead"}

    # golden JSONs (good luminosity sections) shipped with higgs_dna.
    # The 2016/2022/2023/2024 files are inclusive of their sub-eras.
    GOLDEN_JSON = {
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
        metaconditions,
        *,
        apply_trigger: bool = True,
        output_location=None,
        trigger_group: str = ".*DoubleEG.*",
        analysis: str = "mainAnalysis",
        year=None,
        fiducialCuts: str = "classical",
        output_format: str = "parquet",
        nano_version: int = None,
    ) -> None:
        self.meta = metaconditions
        self.apply_trigger = apply_trigger
        self.output_location = output_location
        self.trigger_group = trigger_group
        self.analysis = analysis
        # year is a dict {dataset_name: [year_string]}, like in the base processor
        self.year = year if year is not None else {}
        self.fiducialCuts = fiducialCuts
        self.output_format = output_format
        self.nano_version = nano_version

    # ================================================================== #
    # Step 0: helpers
    # ================================================================== #

    def get_year(self, dataset_name: str) -> str:
        """Return the data-taking year string for this dataset (e.g. '2024')."""
        try:
            return self.year[dataset_name][0]
        except (KeyError, IndexError):
            logger.warning(f"[ inclusive ] No year info for dataset {dataset_name}")
            return None

    def resolve_nano_version(self, events: ak.Array) -> None:
        """Figure out the NanoAOD version if it was not given explicitly."""
        if self.nano_version is not None:
            return
        self.nano_version = infer_nano_version(events)
        if self.nano_version is None:
            raise ValueError(
                "Unable to infer NanoAOD version; pass it explicitly via --nano-version."
            )
        logger.info(f"[ inclusive ] Detected NanoAOD version: {self.nano_version}")

    # ================================================================== #
    # Step 1: luminosity masking (data only)
    # ================================================================== #

    def apply_lumi_mask(self, events: ak.Array, year: str) -> ak.Array:
        """Keep only events in certified (good) luminosity sections."""
        for base in ("2016", "2022", "2023", "2024"):
            if year and base in year:
                year = base
                break

        json_path = golden_json_path(self.GOLDEN_JSON[year])
        lumimask = LumiMask(json_path)
        logger.info(f"[ inclusive ] Year: {year} GoldenJSON: {json_path}")
        return events[lumimask(events.run, events.luminosityBlock)]

    # ================================================================== #
    # Step 2: MET filters + HLT triggers
    # ================================================================== #

    def apply_filters_and_triggers(self, events: ak.Array) -> ak.Array:
        """Apply the recommended MET noise filters and the diphoton HLT paths."""
        # MET filters: every required Flag must be True
        met_filters = self.meta["flashggMetFilters"][self.data_kind]
        filtered = functools.reduce(
            operator.and_,
            (events.Flag[metfilter.split("_")[-1]] for metfilter in met_filters),
        )

        # Triggers: OR of every HLT path in the configured trigger group.
        triggered = ak.ones_like(filtered)
        if self.apply_trigger:
            triggers = self.meta["TriggerPaths"][self.trigger_group][self.analysis]
            hlt = events.HLT
            trigger_names = []
            for trigger in triggers:
                actual = trigger.replace("HLT_", "").replace("*", "")
                for field in hlt.fields:
                    if field.startswith(actual):
                        trigger_names.append(field)
            triggered = functools.reduce(
                operator.or_, (hlt[name] for name in trigger_names)
            )

        return events[filtered & triggered]

    @staticmethod
    def remove_ecal_bad_calib(events: ak.Array) -> ak.Array:
        """
        Reject events affected by the EcalBadCalibCrystal issue (a spurious peak
        in the photon pT spectrum). Only relevant for the affected 2022 runs.
        """
        run_mask = (events.run >= 362433) & (events.run <= 367144)
        met_cut = events.PuppiMET.pt > 100

        dphi = numpy.abs(events.PuppiMET.phi - events.Jet.phi) % (2 * numpy.pi)
        dphi = ak.where(dphi > numpy.pi, 2 * numpy.pi - dphi, dphi)
        jet_cuts = (
            (events.Jet.pt > 50)
            & ((events.Jet.eta > -0.5) & (events.Jet.eta < -0.1))
            & ((events.Jet.phi > -2.1) & (events.Jet.phi < -1.8))
            & ((events.Jet.neEmEF > 0.9) | (events.Jet.chEmEF > 0.9))
            & (dphi > 2.9)
        )
        events_to_remove = run_mask & met_cut & ak.any(jet_cuts, axis=1)
        return events[~events_to_remove]

    # ================================================================== #
    # Step 3: photon-level preparation
    # ================================================================== #

    @staticmethod
    def add_zero_photon_mass_and_charge(photons: ak.Array) -> ak.Array:
        """Photons have no mass/charge in NanoAOD; add zeros so vector ops work."""
        photons["mass"] = ak.zeros_like(photons.pt)
        photons["charge"] = ak.zeros_like(photons.pt)
        return photons

    @staticmethod
    def add_photon_sc_eta(photons: ak.Array, PV: ak.Array) -> ak.Array:
        """
        Add the supercluster eta (``ScEta``) to the photons. In recent NanoAOD
        (v13+) it is stored directly; for older versions it is recomputed from
        the primary-vertex position.
        """
        if "superclusterEta" in photons.fields:
            photons["ScEta"] = photons.superclusterEta
            return photons

        PV_x = PV.x.to_numpy()
        PV_y = PV.y.to_numpy()
        PV_z = PV.z.to_numpy()

        mask_barrel = photons.isScEtaEB
        mask_endcap = photons.isScEtaEE

        tg_theta_over_2 = numpy.exp(-photons.eta)
        tg_theta_over_2 = numpy.where(tg_theta_over_2 == 1.0, 1 - 1e-10, tg_theta_over_2)
        tg_theta = 2 * tg_theta_over_2 / (1 - tg_theta_over_2 * tg_theta_over_2)

        # barrel: project onto a cylinder of radius R = 130 cm
        R = 130.0
        angle_x0_y0 = numpy.zeros_like(PV_x)
        angle_x0_y0[PV_x > 0] = numpy.arctan(PV_y[PV_x > 0] / PV_x[PV_x > 0])
        angle_x0_y0[PV_x < 0] = numpy.pi + numpy.arctan(PV_y[PV_x < 0] / PV_x[PV_x < 0])
        angle_x0_y0[(PV_x == 0) & (PV_y >= 0)] = numpy.pi / 2
        angle_x0_y0[(PV_x == 0) & (PV_y < 0)] = -numpy.pi / 2

        alpha = angle_x0_y0 + (numpy.pi - photons.phi)
        sin_beta = numpy.sqrt(PV_x**2 + PV_y**2) / R * numpy.sin(alpha)
        beta = numpy.abs(numpy.arcsin(sin_beta))
        gamma = numpy.pi / 2 - alpha - beta
        length = numpy.sqrt(
            R**2 + PV_x**2 + PV_y**2 - 2 * R * numpy.sqrt(PV_x**2 + PV_y**2) * numpy.cos(gamma)
        )
        z0_zSC = length / tg_theta
        tg_sctheta = ak.where(mask_barrel, R / (PV_z + z0_zSC), numpy.copy(tg_theta))

        # endcap: project onto the endcap plane at |z| = 310 cm
        intersection_z = numpy.where(photons.eta > 0, 310.0, -310.0)
        r = (intersection_z - PV_z) * tg_theta
        crystalX = PV_x + r * numpy.cos(photons.phi)
        crystalY = PV_y + r * numpy.sin(photons.phi)
        tg_sctheta = ak.where(
            mask_endcap, numpy.sqrt(crystalX**2 + crystalY**2) / intersection_z, tg_sctheta
        )

        sctheta = numpy.arctan(tg_sctheta)
        sctheta = ak.where(sctheta < 0, numpy.pi + sctheta, sctheta)
        photons["ScEta"] = -numpy.log(numpy.tan(sctheta / 2))
        return photons

    def photon_preselection(self, photons: ak.Array, events: ak.Array, year: str) -> ak.Array:
        """
        HLT-mimicking single-photon preselection. Applied per photon (not per
        pair): pT, acceptance, electron veto, MVA ID, H/E, R9 and isolation.
        """
        rho = events.Rho.fixedGridRhoAll * ak.ones_like(photons.pt)
        photon_abs_eta = numpy.abs(photons.eta)

        # photon-isolation requirement with rho (pileup) correction.
        # NOTE: the |eta| bin edges use strict '>' / '<' comparisons (matching
        # the official preselection): a photon sitting exactly on a bin edge
        # (e.g. |eta| == 1.0 or 2.0) fails every bin and is rejected.
        if year in ["2016", "2016PreVFP", "2016PostVFP", "2017", "2018"]:
            # Run2: linear effective-area correction
            pass_phoIso_EB = photons.pfPhoIso03 - rho * 0.16544 < self.max_pho_iso_EB_low_r9
            pass_phoIso_EE = photons.pfPhoIso03 - rho * 0.13212 < self.max_pho_iso_EE_low_r9
        else:
            # Run3: quadratic effective-area correction, binned in |eta|
            pass_phoIso_EB = (
                ((photon_abs_eta > 0.0) & (photon_abs_eta < 1.0) & (photons.pfPhoIso03 - rho * self.EA1_EB1 - rho * rho * self.EA2_EB1 < self.max_pho_iso_EB_low_r9))
                | ((photon_abs_eta > 1.0) & (photon_abs_eta < 1.4442) & (photons.pfPhoIso03 - rho * self.EA1_EB2 - rho * rho * self.EA2_EB2 < self.max_pho_iso_EB_low_r9))
            )
            pass_phoIso_EE = (
                ((photon_abs_eta > 1.566) & (photon_abs_eta < 2.0) & (photons.pfPhoIso03 - rho * self.EA1_EE1 - rho * rho * self.EA2_EE1 < self.max_pho_iso_EE_low_r9))
                | ((photon_abs_eta > 2.0) & (photon_abs_eta < 2.2) & (photons.pfPhoIso03 - rho * self.EA1_EE2 - rho * rho * self.EA2_EE2 < self.max_pho_iso_EE_low_r9))
                | ((photon_abs_eta > 2.2) & (photon_abs_eta < 2.3) & (photons.pfPhoIso03 - rho * self.EA1_EE3 - rho * rho * self.EA2_EE3 < self.max_pho_iso_EE_low_r9))
                | ((photon_abs_eta > 2.3) & (photon_abs_eta < 2.4) & (photons.pfPhoIso03 - rho * self.EA1_EE4 - rho * rho * self.EA2_EE4 < self.max_pho_iso_EE_low_r9))
                | ((photon_abs_eta > 2.4) & (photon_abs_eta < 2.5) & (photons.pfPhoIso03 - rho * self.EA1_EE5 - rho * rho * self.EA2_EE5 < self.max_pho_iso_EE_low_r9))
            )

        # tracker isolation: branch name changed across NanoAOD versions
        iso = photons.trkSumPtHollowConeDR03 if hasattr(photons, "trkSumPtHollowConeDR03") else photons.pfChargedIsoPFPV
        rel_iso = photons.pfRelIso03_chg if hasattr(photons, "pfRelIso03_chg") else photons.pfRelIso03_chg_quadratic

        # High-R9 photons pass automatically; low-R9 ones face the shower-shape cuts.
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

    # ================================================================== #
    # Lepton / jet selection (for the jet-counting output variables)
    # ================================================================== #

    def select_electrons(self, electrons: ak.Array, diphotons: ak.Array) -> ak.Array:
        """Loose, isolated electrons away from both photons (for jet cleaning)."""
        pt_cut = electrons.pt > self.electron_pt_threshold
        eta_cut = abs(electrons.eta) < self.electron_max_eta
        is_transition = (abs(electrons.eta) > 1.4442) & (abs(electrons.eta) < 1.566)
        eta_cut = eta_cut & (~is_transition)
        id_cut = electrons.cutBased >= 2  # loose
        dr_lead = delta_r_mask(electrons, diphotons.pho_lead, self.electron_photon_min_dr)
        dr_sublead = delta_r_mask(electrons, diphotons.pho_sublead, self.electron_photon_min_dr)
        return pt_cut & eta_cut & id_cut & dr_lead & dr_sublead

    def select_muons(self, muons: ak.Array, diphotons: ak.Array) -> ak.Array:
        """Medium, tight-isolated global muons away from both photons (for jet cleaning)."""
        pt_cut = muons.pt > self.muon_pt_threshold
        eta_cut = abs(muons.eta) < self.muon_max_eta
        id_cut = muons.mediumId             # medium
        iso_cut = muons.pfIsoId >= 4        # tight
        global_cut = muons.isGlobal
        dr_lead = delta_r_mask(muons, diphotons.pho_lead, self.muon_photon_min_dr)
        dr_sublead = delta_r_mask(muons, diphotons.pho_sublead, self.muon_photon_min_dr)
        return pt_cut & eta_cut & id_cut & iso_cut & global_cut & dr_lead & dr_sublead

    def select_jets(self, jets: ak.Array, diphotons: ak.Array, muons: ak.Array, electrons: ak.Array) -> ak.Array:
        """tightLepVeto jets passing pT/eta and cleaned against photons/leptons."""
        jetId_cut = jets.jetId == 6  # tightLepVeto
        pt_cut = jets.pt > self.jet_pt_threshold
        eta_cut = abs(jets.eta) < self.jet_max_eta

        if self.clean_jet_pho and (ak.num(diphotons.pt, axis=0) > 0):
            lead = ak.with_name(ak.zip({k: diphotons.pho_lead[k] for k in ("pt", "eta", "phi", "mass", "charge")}), "PtEtaPhiMCandidate")
            sublead = ak.with_name(ak.zip({k: diphotons.pho_sublead[k] for k in ("pt", "eta", "phi", "mass", "charge")}), "PtEtaPhiMCandidate")
            dr_pho_lead = delta_r_mask(jets, lead, self.jet_pho_min_dr)
            dr_pho_sublead = delta_r_mask(jets, sublead, self.jet_pho_min_dr)
        else:
            dr_pho_lead = jets.pt > -1
            dr_pho_sublead = jets.pt > -1

        dr_ele = delta_r_mask(jets, electrons, self.jet_ele_min_dr) if (self.clean_jet_ele and ak.num(electrons.pt, axis=0) > 0) else jets.pt > -1
        dr_muo = delta_r_mask(jets, muons, self.jet_muo_min_dr) if (self.clean_jet_muo and ak.num(muons.pt, axis=0) > 0) else jets.pt > -1

        return jetId_cut & pt_cut & eta_cut & dr_pho_lead & dr_pho_sublead & dr_ele & dr_muo

    def add_jet_variables(self, diphotons: ak.Array, events: ak.Array, year: str) -> ak.Array:
        """
        Build cleaned, pT-ordered jets and attach the jet-counting / leading-jet
        kinematic columns to the (still jagged) diphoton candidates, exactly as
        the base processor does.
        """
        raw = events.Jet
        jets = ak.zip(
            {
                "pt": raw.pt,
                "eta": raw.eta,
                "phi": raw.phi,
                "mass": raw.mass,
                "charge": ak.zeros_like(raw.pt),
                "jetId": add_jetId(raw, self.nano_version, year, flattenUnflatten=True),
                **(
                    {"neHEF": raw.neHEF, "neEmEF": raw.neEmEF, "chEmEF": raw.chEmEF, "muEF": raw.muEF}
                    if self.nano_version == 12 else {}
                ),
                **(
                    {"neHEF": raw.neHEF, "neEmEF": raw.neEmEF, "chMultiplicity": raw.chMultiplicity,
                     "neMultiplicity": raw.neMultiplicity, "chEmEF": raw.chEmEF, "chHEF": raw.chHEF, "muEF": raw.muEF}
                    if self.nano_version >= 13 else {}
                ),
            }
        )
        jets = ak.with_name(jets, "PtEtaPhiMCandidate")

        electrons = ak.with_name(
            ak.zip({
                "pt": events.Electron.pt, "eta": events.Electron.eta, "phi": events.Electron.phi,
                "mass": events.Electron.mass, "charge": events.Electron.charge,
                "cutBased": events.Electron.cutBased,
                "mvaIso_WP90": events.Electron.mvaIso_WP90, "mvaIso_WP80": events.Electron.mvaIso_WP80,
            }),
            "PtEtaPhiMCandidate",
        )

        # replicate the base processor's extra iso cut on muons
        events["Muon"] = events.Muon[events.Muon.pfRelIso03_all < 0.2]
        muons = ak.with_name(
            ak.zip({
                "pt": events.Muon.pt, "eta": events.Muon.eta, "phi": events.Muon.phi,
                "mass": events.Muon.mass, "charge": events.Muon.charge,
                "tightId": events.Muon.tightId, "mediumId": events.Muon.mediumId,
                "looseId": events.Muon.looseId, "isGlobal": events.Muon.isGlobal,
                "pfIsoId": events.Muon.pfIsoId,
            }),
            "PtEtaPhiMCandidate",
        )

        sel_electrons = electrons[self.select_electrons(electrons, diphotons)]
        sel_muons = muons[self.select_muons(muons, diphotons)]

        jets = jets[self.select_jets(jets, diphotons, sel_muons, sel_electrons)]
        jets = jets[ak.argsort(jets.pt, ascending=False)]

        diphotons["n_jets"] = ak.num(jets)
        diphotons["NJ"] = ak.num(jets[(jets.pt > 30) & (numpy.abs(jets.eta) < 2.5)])

        first_jet_pt = choose_jet(jets.pt, 0, -999.0)
        first_jet_eta = choose_jet(jets.eta, 0, -999.0)
        diphotons["PTJ0"] = first_jet_pt
        diphotons["first_jet_eta"] = first_jet_eta
        diphotons["first_jet_phi"] = choose_jet(jets.phi, 0, -999.0)
        diphotons["first_jet_mass"] = choose_jet(jets.mass, 0, -999.0)
        diphotons["first_jet_charge"] = choose_jet(jets.charge, 0, -999.0)

        diphotons["PTJ1"] = choose_jet(jets.pt, 1, -999.0)
        diphotons["second_jet_eta"] = choose_jet(jets.eta, 1, -999.0)
        diphotons["second_jet_phi"] = choose_jet(jets.phi, 1, -999.0)
        diphotons["second_jet_mass"] = choose_jet(jets.mass, 1, -999.0)
        diphotons["second_jet_charge"] = choose_jet(jets.charge, 1, -999.0)

        with numpy.errstate(over="ignore", invalid="ignore"):
            first_jet_pz = first_jet_pt * numpy.sinh(first_jet_eta)
            first_jet_energy = numpy.sqrt((first_jet_pt**2 * numpy.cosh(first_jet_eta) ** 2) + choose_jet(jets.mass, 0, -999.0) ** 2)
            first_jet_y = 0.5 * numpy.log((first_jet_energy + first_jet_pz) / (first_jet_energy - first_jet_pz))
            first_jet_y = ak.fill_none(first_jet_y, -999)
            first_jet_y = ak.where(numpy.isnan(first_jet_y), -999, first_jet_y)
        diphotons["YJ0"] = first_jet_y

        return diphotons

    # ================================================================== #
    # Step 4: diphoton candidate building
    # ================================================================== #

    def build_diphoton_candidates(self, photons: ak.Array) -> ak.Array:
        """Pair up the preselected photons and compute the diphoton kinematics."""
        sorted_photons = photons[ak.argsort(photons.pt, ascending=False)]
        diphotons = ak.combinations(sorted_photons, 2, fields=["pho_lead", "pho_sublead"])

        # leading-photon pT cut
        diphotons = diphotons[diphotons["pho_lead"].pt > self.min_pt_lead_photon]

        # diphoton four-momentum
        diphoton_4mom = diphotons["pho_lead"] + diphotons["pho_sublead"]
        diphotons["pt"] = diphoton_4mom.pt
        diphotons["eta"] = diphoton_4mom.eta
        diphotons["phi"] = diphoton_4mom.phi
        diphotons["mass"] = diphoton_4mom.mass
        diphotons["charge"] = diphoton_4mom.charge
        diphotons["rapidity"] = 0.5 * numpy.log(
            (diphoton_4mom.energy + diphoton_4mom.z) / (diphoton_4mom.energy - diphoton_4mom.z)
        )

        # keep the highest-pT candidate first
        diphotons = diphotons[ak.argsort(diphotons.pt, ascending=False)]
        return ak.with_name(diphotons, "PtEtaPhiMCandidate")

    def apply_fiducial_cut_det_level(self, diphotons: ak.Array) -> ak.Array:
        """Detector-level fiducial selection (classical or geometric)."""
        lead = diphotons.pho_lead
        sublead = diphotons.pho_sublead
        lead_rel_iso = lead.pfRelIso03_all if hasattr(lead, "pfRelIso03_all") else lead.pfRelIso03_all_quadratic
        sublead_rel_iso = sublead.pfRelIso03_all if hasattr(sublead, "pfRelIso03_all") else sublead.pfRelIso03_all_quadratic

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

    # ================================================================== #
    # Step 5: per-event observables
    # ================================================================== #

    @staticmethod
    def compute_sigma_m_over_m(diphotons: ak.Array) -> ak.Array:
        """Relative diphoton mass resolution from the per-photon energy errors."""
        lead, sublead = diphotons.pho_lead, diphotons.pho_sublead
        lead_e = lead.pt * numpy.cosh(lead.eta)
        sublead_e = sublead.pt * numpy.cosh(sublead.eta)
        diphotons["sigma_m_over_m"] = 0.5 * numpy.sqrt(
            (lead.energyErr / lead_e) ** 2 + (sublead.energyErr / sublead_e) ** 2
        )
        return diphotons

    # ================================================================== #
    # Step 6: output
    # ================================================================== #

    def diphoton_to_ak_array(self, diphotons: ak.Array) -> ak.Array:
        """
        Flatten the diphoton record into a flat set of columns: the two photon
        legs get a ``lead_``/``sublead_`` prefix, everything else is kept as-is.
        """
        output = {}
        for field in ak.fields(diphotons):
            prefix = self.prefixes.get(field, "")
            if prefix:
                for subfield in ak.fields(diphotons[field]):
                    if subfield != "__systematics__":
                        output[f"{prefix}_{subfield}"] = diphotons[field][subfield]
            else:
                output[field] = diphotons[field]
        return ak.Array(output)

    def dump_to_parquet(self, akarr: ak.Array, events: ak.Array, metadata: dict, subdirs) -> None:
        """Write the flat diphoton array to ``output_location/<subdirs>/<file>``."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        import pathlib

        fname = events.attrs["@events_factory"]._partition_key.replace("/", "_")
        fname = (fname.replace("%2F", "").replace("%3B1", "")) + f".{self.output_format}"

        destination = "/".join([self.output_location.rstrip("/")] + list(subdirs) + [fname])

        pa_table = ak.to_arrow_table(akarr, extensionarray=False)
        # deterministic column ordering so the files read back consistently
        col_names = sorted(pa_table.schema.names)
        pa_table = pa.table([pa_table.column(n) for n in col_names], names=col_names)
        if metadata:
            merged = {**metadata, **(pa_table.schema.metadata or {})}
            pa_table = pa_table.replace_schema_metadata(merged)

        pathlib.Path(os.path.dirname(destination)).mkdir(parents=True, exist_ok=True)
        pq.write_table(pa_table, destination)

    # ================================================================== #
    # Main entry point
    # ================================================================== #

    def process(self, events: ak.Array) -> dict:
        self.resolve_nano_version(events)
        dataset_name = events.metadata["dataset"]
        self.data_kind = "mc" if hasattr(events, "GenPart") else "data"
        year = self.get_year(dataset_name)

        # ----- bookkeeping: event/weight counts before any selection ----- #
        histos_etc = {dataset_name: {}}
        if self.data_kind == "mc":
            histos_etc[dataset_name]["nTot"] = int(ak.num(events.genWeight, axis=0))
            histos_etc[dataset_name]["nPos"] = int(ak.sum(events.genWeight > 0))
            histos_etc[dataset_name]["nNeg"] = int(ak.sum(events.genWeight < 0))
            histos_etc[dataset_name]["nEff"] = histos_etc[dataset_name]["nPos"] - histos_etc[dataset_name]["nNeg"]
            histos_etc[dataset_name]["genWeightSum"] = float(numpy.sum(events.genWeight.to_numpy()))
        else:
            histos_etc[dataset_name]["nTot"] = int(len(events))
            histos_etc[dataset_name]["nPos"] = int(len(events))
            histos_etc[dataset_name]["nNeg"] = 0
            histos_etc[dataset_name]["nEff"] = int(len(events))
            histos_etc[dataset_name]["genWeightSum"] = float(len(events))

        # ----- Step 1: luminosity mask (data only) ----- #
        if self.data_kind == "data" and year is not None:
            events = self.apply_lumi_mask(events, year)

        metadata = {}
        if self.data_kind == "mc":
            metadata["sum_genw_presel"] = str(numpy.sum(events.genWeight.to_numpy()))
        else:
            metadata["sum_genw_presel"] = "Data"

        # ----- Step 2: MET filters + triggers ----- #
        events = self.apply_filters_and_triggers(events)

        # remove EcalBadCalibCrystal-affected events (data, Run3 only)
        if self.data_kind == "data" and year not in ["2018", "2017", "2016preVFP", "2016postVFP"]:
            events = self.remove_ecal_bad_calib(events)

        # ----- Step 3: photon preparation ----- #
        events["Photon"] = self.add_zero_photon_mass_and_charge(events.Photon)
        events["Photon"] = self.add_photon_sc_eta(events.Photon, events.PV)
        photons = self.photon_preselection(events.Photon, events, year)

        # ----- Step 4: diphoton candidates + fiducial selection ----- #
        diphotons = self.build_diphoton_candidates(photons)
        diphotons = self.apply_fiducial_cut_det_level(diphotons)

        # ----- Step 5: jet-counting / leading-jet variables ----- #
        # done while diphotons is still jagged, so jet cleaning sees every
        # diphoton candidate of the event (matching the base processor)
        diphotons = self.add_jet_variables(diphotons, events, year)

        # one candidate per event (highest pT), then drop events without one
        diphotons = ak.firsts(diphotons)
        selection_mask = ~ak.is_none(diphotons)
        diphotons = diphotons[selection_mask]
        sel_events = events[selection_mask]

        if len(diphotons) == 0:
            logger.info("[ inclusive ] No surviving events in this chunk.")

        # ----- Step 6: annotate with per-event info + weights ----- #
        diphotons["event"] = sel_events.event
        diphotons["lumi"] = sel_events.luminosityBlock
        diphotons["run"] = sel_events.run
        diphotons["nPV"] = sel_events.PV.npvs
        diphotons["fixedGridRhoAll"] = sel_events.Rho.fixedGridRhoAll
        diphotons["BeamSpot_sigmaZ"] = sel_events.BeamSpot.sigmaZ
        diphotons["BeamSpot_sigmaZError"] = sel_events.BeamSpot.sigmaZError

        if self.data_kind == "mc":
            diphotons["genWeight"] = sel_events.genWeight
            diphotons["dZ"] = sel_events.GenVtx.z - sel_events.PV.z
            diphotons["weight"] = sel_events.genWeight
            diphotons["weight_central"] = ak.ones_like(sel_events.genWeight)
            metadata["sum_weight_central"] = str(ak.sum(sel_events.genWeight))
            # particle-level truth + fiducial flags (for acceptance / unfolding)
            diphotons["fiducialClassicalFlag"] = get_fiducial_flag(sel_events, flavour="Classical")
            diphotons["fiducialGeometricFlag"] = get_fiducial_flag(sel_events, flavour="Geometric")
            TruthPTH, TruthYH = get_higgs_truth_attributes(sel_events)
            diphotons["TruthPTH"] = TruthPTH
            diphotons["TruthYH"] = TruthYH
        else:
            diphotons["dZ"] = ak.zeros_like(sel_events.PV.z)
            diphotons["weight"] = ak.ones_like(diphotons["event"])
            diphotons["weight_central"] = ak.ones_like(diphotons["event"])

        diphotons = self.compute_sigma_m_over_m(diphotons)

        # ----- Step 6: write out ----- #
        if self.output_location is not None:
            akarr = self.diphoton_to_ak_array(diphotons)
            # drop the per-photon copy of the event-level rho
            akarr = akarr[[f for f in akarr.fields if "lead_fixedGridRhoAll" not in f]]
            self.dump_to_parquet(akarr, events, metadata, subdirs=[dataset_name, "nominal"])

        return histos_etc

    def postprocess(self, accumulant: dict) -> None:
        pass


# ====================================================================== #
# Runner: read the whole 2024 GluGluH signal file and dump the parquet.
# ====================================================================== #

if __name__ == "__main__":
    import time

    from coffea.nanoevents import NanoEventsFactory

    INPUT_FILE = "GluGluH_Hto2G_2024v15.root"
    DATASET = "MC"
    YEAR = "2024"
    OUTPUT_DIR = "output_inclusive"

    # metaconditions (triggers + MET filters) loaded from the higgs_dna package
    with resources.files("higgs_dna.metaconditions").joinpath("Era2022_v1.json").open("r") as f:
        metaconditions = json.load(f)

    path = os.path.abspath(INPUT_FILE)

    processor_instance = HggInclusiveProcessor(
        metaconditions=metaconditions,
        output_location=OUTPUT_DIR,
        year={DATASET: [YEAR]},
    )

    # entry_stop=None reads the whole file
    events = NanoEventsFactory.from_root(
        {path: "Events"},
        metadata={"dataset": DATASET, "filename": path.split("/")[-1]},
        entry_stop=None,
    ).events()

    start = time.time()
    out = processor_instance.process(events)
    end = time.time()
    print(out)
    print(f"Processing time: {end - start:.2f} seconds")
