from .ontology import ONTOLOGY_MAPPING, construct_ontology_mapping, get_ontology


def test_construct_ontology_mapping():
    equiv_dict = construct_ontology_mapping()
    assert equiv_dict
    assert equiv_dict["mean_daily_flow"] == get_ontology(
        "http://vocabulary.odm2.org/api/v1/variablename/waterLevel"
    )

    assert ONTOLOGY_MAPPING["mean_daily_flow_available"] == get_ontology(
        "http://vocabulary.odm2.org/api/v1/variablename/waterLevel"
    )
    assert equiv_dict["mean_daily_flow"].id != equiv_dict["water temperature"].id
    assert (
        equiv_dict["mean_daily_flow"].id == 44596
    ), "The deterministic hash that generates the id should always generate this number. This is either an extremely rare scenario or a different name was used to generate the id"
