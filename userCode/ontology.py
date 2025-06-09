# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

import json
from typing import Optional, Sequence
from dagster import get_dagster_logger
from pydantic import BaseModel, Field

from userCode.cache import ShelveCache
from userCode.awqms._generated.schema import ResourceURI
from userCode.util import deterministic_hash


class Ontology(BaseModel, extra="forbid"):
    # The human readable name of the vocabulary term
    name: str
    # The human readable description of the vocabulary term
    definition: str
    # Some sort of associated info about the term such as the provenance description
    description: str
    # The url / uri that a client could go to for more information
    uri: str
    # a unique id that can be used in the sensorthings api to differentiate the term
    id: int = Field(
        alias="@iot.id",
        default_factory=lambda input: deterministic_hash(input["uri"], 5),
    )


def get_ontology(uri: str) -> Ontology:
    """Parse an odm2 vocabulary into a pydantic model"""
    uriAsJson = f"{uri}?format=json"
    get_dagster_logger().info(f"Constructing ontology object from {uriAsJson}")
    cache = ShelveCache()
    # always cache the ontology
    resp, status = cache.get_or_fetch(uriAsJson, force_fetch=False, cache_result=True)
    assert status == 200, (
        f"Request to {uriAsJson} failed with status {status} and body {resp}"
    )
    asJson = json.loads(resp)
    return Ontology(
        definition=asJson["definition"],
        description=asJson["provenance"],
        name=asJson["name"],
        uri=uri,
    )


# Define the association between user terms
__ontology_definition: dict[Sequence, Optional[ResourceURI]] = {
    (
        "mean_daily_flow",
        "mean daily flow",
        "mean_daily_flow_available",
        "Stream gage",
    ): "streamflow",
    (
        "water_temp_mean",
        "water temperature",
        "water_temp_mean_available",
        "Temperature, water",
    ): "temperatureSensor",
    (
        "groundwater_level",
        "groundwater depth",
        "groundwater",
    ): "groundwaterDepth",
    ("Alkalinity, total"): "alkalinityTotal",
    # this is electrical and not hydr
    ("Conductivity"): "electricalConductivity",
    ("Dissolved oxygen (DO)"): "oxygenDissolved",
    ("Dissolved oxygen saturation"): "oxygenDissolvedPercentOfSaturation",
    ("pH"): "pH",
    ("Turbidity", "Turbidity Field"): "turbidity",
    ("Aluminum"): "aluminum",
    ("Ammonia"): "nitrogen_NH3",
    ("Arsenic"): "arsenic",
    ("Biochemical oxygen demand, non-standard conditions"): None,
    ("Cadmium"): "cadmium",
    ("Calcium"): "calciumTotal",
    ("Chemical oxygen demand"): None,
    ("Chlorophyll a"): "chlorophyll_a",
    ("Chromium"): "chromium",
    ("Copper"): "copper",
    ("Escherichia coli"): "e_coli",
    ("Fecal Coliform"): "coliformFecal",
    # NOTE: this could map to multiple, confusing?
    ("Hardness, Ca, Mg"): "hardnessCalcium",
    ("Iron"): "Iron",
    ("Lead"): "lead",
    ("Magnesium"): "magnesium",
    ("Nickel"): "nickel",
    ("Nitrate + Nitrite"): "nitrogenNitrite_NO2_Nitrate_NO3",
    ("Organic carbon"): "carbonOrganicExtractable",
    ("Orthophosphate"): "phosphorusOrthophosphate",
    ("Pheophytin a"): "pheophytin",
    ("Potassium"): "potassium",
    ("Selenium"): "seleniumTotal",
    ("Silver"): "silver",
    ("Sodium"): "sodium",
    ("Total Kjeldahl nitrogen"): "nitrogenTotalKjeldahl",
    ("Total Phosphorus, mixed forms"): "phosphorusTotal",
    ("Total solids"): "solidsTotal",
    ("Total suspended solids"): "solidsTotalSuspended",
    ("Zinc"): "zinc",
    ("Specific conductance"): "specificConductance",
    ("Barium"): "barium",
    ("Beryllium"): "beryllium",
    ("Boron"): "boronTotal",
    ("Bromide"): "bromide",
    ("Chloride"): "chloride",
    ("Cobalt"): "cobalt",
    ("Lanthanum"): "lanthanum",
    ("Lithium"): "lithiumTotal",
    ("Manganese"): "manganese",
    ("Molybdenum"): "molybdenumTotal",
    ("Perchlorate"): None,
    ("Silica"): "silica",
    ("Sulfate"): "sulfate",
    ("Total dissolved solids"): "solidsTotalDissolved",
    ("Vanadium"): "vanadiumTotal",
    ("Water level (probe)"): "waterLevel",
    ("Count"): None,
    ("Density"): "density",
    ("Depth, bottom"): "depth",
    ("Depth, from ground surface to well water level"): "groundwaterDepth",
    ("Chlorthal-dimethyl"): None,
    ("Dacthal Acid Metabolites"): None,
    ("Depth, data-logger (ported)"): "depth",
    ("Atrazine"): None,
    ("Azinphos-methyl"): None,
    ("Azinphos-methyl oxygen analog"): None,
    ("Chlorpyrifos"): None,
    ("Chlorpyrifos O.A."): None,
    ("Diazinon"): None,
    ("Dimethoate"): None,
    ("Ethoprop"): None,
    ("Fenvalerate + Esfenvalerate"): None,
    ("Malaoxon"): None,
    ("Malathion"): "malathion",
    ("Methyl parathion"): None,
    ("Phosmet"): None,
    ("Phosmetoxon"): None,
    ("Pyriproxyfen"): None,
    ("Simazine"): None,
    ("Thallium"): "thallium",
    ("Total Coliform"): "coliformTotal",
    ("Antimony"): "antimony",
    ("Halogenated organics"): None,
    ("True color"): "color",
    ("Dicamba"): None,
    ("Enterococcus"): "enterococci",
    # NOTE" unsure on this if gauge or something else
    ("Pressure"): "pressureGauge",
    ("Salinity"): "salinity",
    ("Nitrate"): "nitrogenTotalNitrite",
    ("Nitrite"): "nitrogenTotalNitrite",
    ("Disulfoton"): "disulfoton",
    ("Phorate"): "phorate",
    ("Flow"): "wellFlowRate",
    (
        "Carbonaceous biochemical oxygen demand, non-standard conditions"
    ): "BOD2Carbonaceous",
    (".alpha.-Endosulfan"): "endosulfan_I_Alpha",
    (".alpha.-Hexachlorocyclohexane"): None,
    (".beta.-Endosulfan"): "endosulfan_II_Beta",
    (".beta.-Hexachlorocyclohexane"): None,
    (".delta.-Hexachlorocyclohexane"): None,
    ("2,6-Dichlorobenzamide"): "2_6_Dichlorophenol",
    ("2-Chloro-4-isopropylamino-6-amino-s-triazine"): None,
    ("Acetamiprid"): None,
    ("Acetochlor"): None,
    ("Alachlor"): None,
    ("Aldrin"): "aldrin",
    ("Ametryn"): None,
    ("Bifenthrin"): "bifenthrin",
    ("Bromacil"): None,
    ("Butachlor"): None,
    ("Butylate"): None,
    ("Carbaryl"): None,
    ("Carbofuran"): None,
    ("Chlorobenzilate"): "chlorobenzilate",
    ("Chloroneb"): None,
    ("Chlorothalonil"): None,
    ("Chlorpropham"): None,
    ("cis-Chlordane"): None,
    ("Cyanazine"): None,
    ("Cycloate"): None,
    ("Deisopropylatrazine"): None,
    ("Dichlobenil"): None,
    ("Dichlorvos"): None,
    ("Dieldrin"): "dieldrin",
    ("Dimethenamid"): None,
    ("Diphenamid"): None,
    ("Diuron"): None,
    ("Endosulfan sulfate"): "endosulfanSulfate",
    ("Endrin"): "endrin",
    ("Endrin aldehyde"): "endrinAldehyde",
    ("Etridiazole"): None,
    ("Fenamiphos"): None,
    ("Fenarimol"): None,
    ("Fluometuron"): None,
    ("Fluridone"): None,
    ("Heptachlor"): "heptachlor",
    ("Heptachlor epoxide"): "heptachlorEpoxide",
    ("Hexazinone"): None,
    ("Imazapyr"): None,
    ("Imidacloprid"): None,
    ("Lindane"): None,
    ("Linuron"): None,
    ("Methiocarb"): None,
    ("Methomyl"): None,
    ("Methoxychlor"): "methoxychlor",
    ("Methyl paraoxon"): None,
    ("Metolachlor"): None,
    ("Metribuzin"): None,
    ("Metsulfuron-methyl"): None,
    ("Mevinphos"): "mevinphos",
    ("Mexacarbate"): None,
    ("Mirex"): None,
    ("Molinate"): None,
    ("N,N-Diethyl-m-toluamide"): None,
    ("N-2-Ethylhexylbicycloheptenedicarboximide"): None,
    ("Napropamide"): None,
    ("Neburon"): None,
    ("Norflurazon"): None,
    ("Oxamyl"): None,
    ("Oxyfluorfen"): None,
    ("p,p'-DDD"): "4_4_DDD",
    ("p,p'-DDE"): "4_4_DDE",
    ("p,p'-DDT"): "4_4_DDT",
    # NOTE: Unclear if this parathion_ethyl would fit here
    ("Parathion"): None,
    ("Pebulate"): None,
    ("Pendimethalin"): None,
    ("Permethrin"): "permethrin",
    ("Prometon"): None,
    ("Prometryn"): None,
    ("Pronamide"): "pronamide",
    ("Propachlor"): None,
    ("Propazine"): None,
    ("Propiconazole"): None,
    ("Propoxur"): None,
    ("Pyraclostrobin"): None,
    ("S-Ethyl dipropylthiocarbamate"): None,
    ("Siduron"): None,
    ("Simetryn"): None,
    ("Sulfometuron methyl"): None,
    ("Tebuthiuron"): None,
    ("Terbacil"): None,
    ("Terbufos"): "terbufos",
    ("Terbuthylazine"): None,
    ("Terbutryn"): None,
    ("Tetrachlorvinphos"): None,
    ("trans-Chlordane"): None,
    ("trans-Nonachlor"): None,
    ("Triadimefon"): None,
    ("Tricyclazole"): None,
    ("Trifluralin"): "trifluralin",
    ("Vernolate"): None,
    # NOTE: not sure if there is something more specific
    ("Chlorophyll a (probe relative fluorescence)"): "chlorophyll_a",
    (
        "Phycocyanin (probe relative fluorescence)"
    ): "blue_GreenAlgae_Cyanobacteria_Phycocyanin",
    # NOTE: this might be an issue that its the same as the other
    ("Chlorophyll a (probe)"): "chlorophyll_a",
    ("Phycocyanin"): "blue_GreenAlgae_Cyanobacteria_Phycocyanin",
    ("Depth"): "depth",
    ("Depth, Secchi disk depth"): "secchiDepth",
    ("Silicon"): "silicon",
    ("2,4,5-T"): None,
    ("2,4-D"): None,
    ("2,4-DB"): None,
    ("3,5-Dichlorobenzoic acid"): None,
    ("Acephate"): "acetate",
    ("Acifluorfen"): None,
    ("Aminocarb"): None,
    ("Azoxystrobin"): None,
    ("Carboxin"): None,
    ("Chlorthal monoacid and diacid degradates"): None,
    # NOTE: not sure if this is the same
    ("Dichlorprop"): "1_2_Dichloropropane",
    ("Dinoseb"): "dinoseb",
    ("MCPA"): None,
    ("Mecoprop-p (mcpp-p)"): None,
    ("Pentachlorophenol"): "pentachlorophenol",
    ("Picloram"): None,
    ("Pyraflufen-ethyl"): None,
    ("Silvex"): None,
    ("Triclopyr"): None,
    ("Trifloxystrobin"): None,
    ("Tris(1,3-dichloro-2-propyl)phosphate"): None,
    ("Tris(2-chloroethyl) phosphate"): None,
    ("Nitrogen"): "nitrogen",
    ("Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)"): "nitrogen",
    ("(RS)-AMPA (Aminomethyl phosphonic acid)"): None,
    ("Coumaphos"): None,
    ("Demeton-S"): None,
    ("Fensulfothion"): None,
    ("Fenthion"): None,
    ("Glyphosate"): None,
    ("O-Ethyl O-(p-nitrophenyl) phenylphosphonothioate"): None,
    ("Prothiofos"): None,
    ("Ronnel"): None,
    ("Sulprofos"): None,
    ("Trichloronate"): None,
}


def construct_ontology_mapping() -> dict[str, Ontology]:
    """Construct a dictionary from the association list in which we defined in the ontology mapping"""
    equiv_dict = dict()
    for keys, value in __ontology_definition.items():
        if type(keys) is not tuple:
            keys = (keys,)

        # special case that has to be skipped
        # since there is no standard uri
        # by skipping this it doesn't mean it won't show
        # up at all, rather it means we use the direct name and
        # not the mapped vocabulary term since to do would be ambiguous
        if value and "pheophytin" in value:
            continue

        for key in keys:
            if not value:
                equiv_dict[key] = None
                continue

            assert key not in equiv_dict, (
                f"Tried to add duplicate key {key} when it already exists in {equiv_dict[key]}"
            )
            equiv_dict[key] = get_ontology(
                f"http://vocabulary.odm2.org/api/v1/variablename/{value}/"
            )

    return equiv_dict


ONTOLOGY_MAPPING = construct_ontology_mapping()
