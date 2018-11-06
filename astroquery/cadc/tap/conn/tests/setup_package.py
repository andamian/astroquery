# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
=============
TAP plus
=============

"""
from __future__ import absolute_import

import os


def get_pakcage_data():
    paths = [os.path.join('data', '*.vot'),
             os.path.join('data', '*.pem'),
             os.path.join('data', '*.txt'),
             ] 
    return {'astroquery.cadc.tap.conn.tests': paths}
