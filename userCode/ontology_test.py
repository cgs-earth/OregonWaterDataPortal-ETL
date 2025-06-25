# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Licensed under the MIT License.
#
# =================================================================

from userCode.ontology import (
    construct_ontology_mapping,
    get_or_generate_ontology,
    get_ontology,
)


def test_construct_ontology_mapping():
    equiv_dict = construct_ontology_mapping()
    assert equiv_dict
    assert equiv_dict["mean_daily_flow"] == get_ontology(
        "http://vocabulary.odm2.org/api/v1/variablename/streamflow/"
    )

    assert get_or_generate_ontology()["mean_daily_flow_available"] == get_ontology(
        "http://vocabulary.odm2.org/api/v1/variablename/streamflow/"
    )
    assert equiv_dict["mean_daily_flow"].id != equiv_dict["water temperature"].id
    id = 65662
    assert equiv_dict["mean_daily_flow"].id == id, (
        f"The deterministic hash that generates the id should always generate {id}. A different name was likely used to generate the id"
    )

    assert (
        "Carboxin" in get_or_generate_ontology()
        and "Carbaryl" in get_or_generate_ontology()
    )
