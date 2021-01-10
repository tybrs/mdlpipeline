from mldpipeline.utils.mdltools.mdl_connect import *
from mldpipeline.utils.sqltools.pc_connect import *
from mldpipeline.utils.sqltools.queries import *
from mldpipeline.utils.sqltools.query_tools import *
from mldpipeline.sync.enrollments import SyncEnrollments

import json
import pytest 

with open('../data/wc_pc_mapping_wi21.json', 'r') as f:
    MAPPING_JSON = json.load(f)

RESULTS = []

@pytest.mark.parametrize("course", MAPPING_JSON.keys())
def test_roster(course):
    pass