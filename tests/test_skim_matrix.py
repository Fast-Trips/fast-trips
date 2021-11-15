"""Test skimming config parsing.
"""
import os
import uuid
import numpy as np
import openmatrix as omx

import pytest

from fasttrips.Skimming import Skim

test_matrix = np.array([[1.0, 2.0, 3.0], [1.0, 2.0, 3.0], [1.0, 2.0, 3.0]])
test_name = "skimmy"
num_zones = test_matrix.shape[0]
index_to_zone_ids = ["z1", "z2", "z3"]
test_additional_attribs = {'foo': 'bar'}


@pytest.fixture()
def skim():
    yield Skim(test_name, num_zones, test_matrix, index_to_zone_ids)


def test_skim_writing(skim, tmp_path):
    tmp_dir = tmp_path / str(uuid.uuid4())
    os.mkdir(tmp_dir)

    skim.write_to_file(file_root=tmp_dir, attributes=test_additional_attribs)
    with omx.open_file(tmp_dir / "skimmy.omx", 'r') as f:
        assert np.alltrue(np.array(f) == test_matrix)
        assert f[test_name].attrs.num_zones == test_matrix.shape[0]
        assert f[test_name].attrs.name == test_name
        assert f[test_name].attrs.index_to_zone_ids == index_to_zone_ids
        assert f[test_name].attrs.foo == "bar"






