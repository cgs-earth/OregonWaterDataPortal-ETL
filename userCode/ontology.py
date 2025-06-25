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
from typing import Final, Sequence
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
__ontology_definition: Final[dict[Sequence[str], ResourceURI]] = {
    (
        "mean_daily_flow",
        "mean daily flow",
        "mean_daily_flow_available",
        "Stream gage",
        "Flow",
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
    ("Cadmium"): "cadmium",
    ("Calcium"): "calciumTotal",
    ("Chlorophyll a"): "chlorophyll_a",
    ("Chromium"): "chromium",
    ("Copper"): "copper",
    ("Escherichia coli"): "e_coli",
    ("Fecal Coliform"): "coliformFecal",
    ("Hardness, Ca, Mg"): "hardnessNonCarbonate",
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
    ("Silica"): "silica",
    ("Sulfate"): "sulfate",
    ("Total dissolved solids"): "solidsTotalDissolved",
    ("Vanadium"): "vanadiumTotal",
    ("Water level (probe)"): "waterLevel",
    ("Density"): "density",
    ("Depth, bottom"): "depth",
    ("Depth, from ground surface to well water level"): "groundwaterDepth",
    ("Depth, data-logger (ported)"): "depth",
    ("Malathion"): "malathion",
    ("Thallium"): "thallium",
    ("Total Coliform"): "coliformTotal",
    ("Antimony"): "antimony",
    ("True color"): "color",
    ("Enterococcus"): "enterococci",
    ("Pressure"): "pressureAbsolute",
    ("Salinity"): "salinity",
    ("Nitrate"): "nitrogenNitrate_NO3",
    ("Nitrite"): "nitrogenNitrite_NO2",
    ("Disulfoton"): "disulfoton",
    ("Phorate"): "phorate",
    (".alpha.-Endosulfan"): "endosulfan_I_Alpha",
    (".beta.-Endosulfan"): "endosulfan_II_Beta",
    ("2,6-Dichlorobenzamide"): "2_6_Dichlorophenol",
    ("Aldrin"): "aldrin",
    ("Bifenthrin"): "bifenthrin",
    ("Chlorobenzilate"): "chlorobenzilate",
    ("Dieldrin"): "dieldrin",
    ("Endosulfan sulfate"): "endosulfanSulfate",
    ("Endrin"): "endrin",
    ("Endrin aldehyde"): "endrinAldehyde",
    ("Heptachlor"): "heptachlor",
    ("Heptachlor epoxide"): "heptachlorEpoxide",
    ("Methoxychlor"): "methoxychlor",
    ("p,p'-DDD"): "4_4_DDD",
    ("p,p'-DDE"): "4_4_DDE",
    ("p,p'-DDT"): "4_4_DDT",
    ("Permethrin"): "permethrin",
    ("Pronamide"): "pronamide",
    ("Terbufos"): "terbufos",
    ("Trifluralin"): "trifluralin",
    ("Chlorophyll a (probe relative fluorescence)"): "chlorophyll_a",
    (
        "Phycocyanin (probe relative fluorescence)"
    ): "blue_GreenAlgae_Cyanobacteria_Phycocyanin",
    ("Chlorophyll a (probe)"): "chlorophyll_a",
    ("Phycocyanin"): "blue_GreenAlgae_Cyanobacteria_Phycocyanin",
    ("Depth"): "depth",
    ("Depth, Secchi disk depth"): "secchiDepth",
    ("Silicon"): "silicon",
    ("Acephate"): "acetate",
    ("Dichlorprop"): "1_2_Dichloropropane",
    ("Dinoseb"): "dinoseb",
    ("Pentachlorophenol"): "pentachlorophenol",
    ("Nitrogen"): "nitrogen",
    ("Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)"): "nitrogenTotal",
    ("Stream stage"): "gageHeight",
}

# all terms in this ontology definition are not in ODM2
# and are defined here for our own use
__ontology_definition_with_custom_vocab: Final[dict[Sequence[str], str]] = {
    ("Trichloronate"): "trichloronate",
    ("Chemical oxygen demand"): "Chemical_oxygen_demand",
    (
        "Carbonaceous biochemical oxygen demand, non-standard conditions"
    ): "BOD_nonstandard",
    ("Chlorthal-dimethyl"): "Chlorthal_dimethyl",
    ("Dacthal Acid Metabolites"): "Dacthal_Acid_Metabolites",
    ("Perchlorate"): "Perchlorate",
    ("Count"): "Count",
    ("Halogenated organics"): "Halogenated organics",
    ("Atrazine"): "Atrazine",
    ("Azinphos_methyl", "Azinphos-methyl"): "Azinphos_methyl",
    ("Chlorpyrifos"): "Chlorpyrifos",
    ("Diazinon"): "Diazinon",
    ("Ronnel"): "ronnel",
    ("Sulprofos"): "sulprofos",
    ("Dimethoate"): "dimethoate",
    ("Ethoprop"): "Ethoprop",
    ("Methyl parathion"): "Methyl_parathion",
    ("Phosmet"): "Phosmet",
    (".alpha.-Hexachlorocyclohexane"): "alpha_Hexachlorocyclohexane",
    (".beta.-Hexachlorocyclohexane"): "beta_Hexachlorocyclohexane",
    (".delta.-Hexachlorocyclohexane"): "delta_Hexachlorocyclohexane",
    ("2,4,5-T"): "2_4_5_T",
    ("Simazine"): "Simazine",
    ("2,4-D"): "2_4_D",
    ("2,4-DB"): "2_4_DB",
    (
        "2-Chloro-4-isopropylamino-6-amino-s-triazine"
    ): "2_Chloro_4_isopropylamino_6_amino_s_triazine",
    ("Prothiofos"): "prothiofos",
    ("Glyphosate"): "glyphosate",
    ("Fenthion"): "fenthion",
    (
        "O-Ethyl O-(p-nitrophenyl) phenylphosphonothioate"
    ): "O_Ethyl_O_p_nitrophenyl_phenylphosphonothioate",
    ("Fensulfothion"): "fensulfothion",
    ("3,5-Dichlorobenzoic acid"): "3_5_Dichlorobenzoic_acid",
    ("Coumaphos"): "coumaphos",
    ("Acetamiprid"): "acetamiprid",
    ("Picloram"): "picloram",
    ("Pyraflufen-ethyl"): "pyraflufen_ethyl",
    ("Silvex"): "silvex",
    ("Triclopyr"): "triclopyr",
    ("Trifloxystrobin"): "trifloxystrobin",
    ("Tris(1,3-dichloro-2-propyl)phosphate"): "tris_1_3_dichloro_2_propyl_phosphate",
    ("(RS)-AMPA (Aminomethyl phosphonic acid)"): "RS_AMPA_Aminomethyl_phosphonic_acid",
    ("Tris(2-chloroethyl) phosphate"): "tris_2_chloroethyl_phosphate",
    ("MCPA"): "MCPA",
    ("Mecoprop-p (mcpp-p)"): "mecoprop_p_mcpp_p",
    ("Acifluorfen"): "acifluorfen",
    ("Aminocarb"): "aminocarb",
    ("Terbuthylazine"): "terbuthylazine",
    ("Terbutryn"): "terbutryn",
    ("Tetrachlorvinphos"): "tetrachlorvinphos",
    ("trans-Chlordane"): "trans_chlordane",
    ("trans-Nonachlor"): "trans_nonachlor",
    ("Triadimefon"): "triadimefon",
    ("Tricyclazole"): "tricyclazole",
    ("Vernolate"): "vernoloate",
    ("Acetochlor"): "acetochlor",
    ("Alachlor"): "alachlor",
    ("Ametryn"): "ametryn",
    ("Azinphos-methyl oxygen analog"): "azinphos_methyl_oxygen_analog",
    ("Demeton-S"): "demeton_S",
    ("Terbacil"): "terbacil",
    ("Tebuthiuron"): "tebuthiuron",
    ("Propachlor"): "propachlor",
    ("Propazine"): "propazine",
    ("Propiconazole"): "propiconazole",
    ("Propoxur"): "propoxur",
    ("Pyraclostrobin"): "pyraclostrobin",
    ("S-Ethyl dipropylthiocarbamate"): "S_Ethyl_dipropylthiocarbamate",
    ("Siduron"): "siduron",
    ("Simetryn"): "simetryn",
    ("Sulfometuron methyl"): "sulfometuron methyl",
    ("Phosmetoxon"): "phosmetoxon",
    ("Pyriproxyfen"): "pyriproxyfen",
    ("Prometryn"): "prometryn",
    ("Prometon"): "prometon",
    ("Pendimethalin"): "pendimethalin",
    ("Pebulate"): "pebulate",
    ("Methyl paraoxon"): "methyl_paraoxon",
    ("Metolachlor"): "metolachlor",
    ("Metribuzin"): "metribuzin",
    ("Metsulfuron-methyl"): "metsulfuron_methyl",
    ("Mevinphos"): "mevinphos",
    ("Mexacarbate"): "mexacarbate",
    ("Mirex"): "mirex",
    ("Molinate"): "molinate",
    ("N,N-Diethyl-m-toluamide"): "N_N_Diethyl_m_toluamide",
    (
        "N-2-Ethylhexylbicycloheptenedicarboximide"
    ): "N_2_Ethylhexylbicycloheptenedicarboximide",
    ("Napropamide"): "napropamide",
    ("Neburon"): "neburon",
    ("Norflurazon"): "norflurazon",
    ("Oxamyl"): "oxamyl",
    ("Oxyfluorfen"): "oxyfluorfen",
    ("Parathion"): "parathion",
    ("Methomyl"): "methomyl",
    ("Hexazinone"): "hexazinone",
    ("Imazapyr"): "imazapyr",
    ("Imidacloprid"): "imidacloprid",
    ("Lindane"): "lindane",
    ("Linuron"): "linuron",
    ("Methiocarb"): "methiocarb",
    ("Malaoxon"): "malaoxon",
    ("Etridiazole"): "etridiazole",
    ("Fenamiphos"): "fenamiphos",
    ("Fenarimol"): "fenarimol",
    ("Fluometuron"): "fluometuron",
    ("Fluridone"): "fluridone",
    ("Chlorpyrifos O.A."): "chlorpyrifos_o_a",
    ("Fenvalerate + Esfenvalerate"): "fenvalerate_and_esfenvalerate",
    ("Dimethenamid"): "dimethenamid",
    ("Diphenamid"): "diphenamid",
    ("Diuron"): "diuron",
    ("Chloroneb"): "chloroneb",
    ("Chlorothalonil"): "chlorothalonil",
    ("Chlorpropham"): "chlorpropham",
    ("cis-Chlordane"): "cis-chlordane",
    ("Cyanazine"): "cyanazine",
    ("Cycloate"): "cycloate",
    ("Deisopropylatrazine"): "deisopropylatrazine",
    ("Dichlobenil"): "dichlobenil",
    ("Dichlorvos"): "dichlorvos",
    ("Dicamba"): "dicamba",
    ("Bromacil"): "bromacil",
    ("Butachlor"): "butachlor",
    ("Butylate"): "butylate",
    ("Carbaryl"): "carbaryl",
    ("Carbofuran"): "carbofuran",
    ("Azoxystrobin"): "azoxystrobin",
    ("Carboxin"): "carboxin",
    (
        "Chlorthal monoacid and diacid degradates"
    ): "chlorthal_monoacid_and_diacid_degradate",
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

    for keys, value in __ontology_definition_with_custom_vocab.items():
        if type(keys) is not tuple:
            keys = (keys,)

        assert value

        for key in keys:
            assert isinstance(key, str)

            ontology = Ontology(
                name=key,
                definition=value,
                description=value,
                uri=f"http://vocabulary.odm2.org/api/v1/variablename/{value}/",
            )
            equiv_dict[key] = ontology

    return equiv_dict


ONTOLOGY_MAPPING = construct_ontology_mapping()
print("Ontology mapping constructed")
