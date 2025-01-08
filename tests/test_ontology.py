# =================================================================
#
# Authors: Colton Loftus <cloftus@lincolninst.edu>
#
# Copyright (c) 2025 Lincoln Institute of Land Policy
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from userCode.ontology import (
    ONTOLOGY_MAPPING,
    construct_ontology_mapping,
    get_ontology,
)


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
        equiv_dict["mean_daily_flow"].id == 52274
    ), "The deterministic hash that generates the id should always generate this number. This is either an extremely rare scenario or a different name was used to generate the id"
