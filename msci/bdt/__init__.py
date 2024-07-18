"""
The msci.bdt package exposes a series of modules to interact with the Barra Development Toolkit (BDT) web services.

The package provides modules to interact with both the service and interactive end-points in BDT.

The context sub-package contains the core functionality to interact with BDT while the msci.bdt modules implement
specific cases of interaction covering:

* Get/Download exposures
* Specific Correlation/Covariance calculation and retrieval.
* Importing portfolios and MPC cases
* Interactive positions reports.

It is strongly recommended to start working with msic.bdt based on the provided examples.

Please navigate to specific package/module for details.

"""
import os
import random

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

VALID_EXPORT_SET_NAME_CHARACTERS = "_-$%@*^("
def keep_valid_characters(name):
    l = [ss for ss in name if ss.isalnum() or (ss in VALID_EXPORT_SET_NAME_CHARACTERS)]
    return "".join(l)

def create_unique_export_set_name(portfolio='', model=''):
    """
    Creates an export-set name, which is a valid Barra export-set name and is different each time it is called.
    The idea is to avoid pre-defined export-set names in the code, so that parallel runs (either by the same or different users)
    do not interfere with each other.

    :param portfolio:
    :param model:
    :return:
    """
    export_set_name = '%.10s_%.5s_%i%i' % (portfolio.replace(' ', '').replace('.', ''),
                                      model.replace(' ', '').replace('.', ''),
                                      random.randint(0, 9999),
                                      os.getpid())
    export_set_name = keep_valid_characters(export_set_name)
    export_set_name = export_set_name[:25]
    return export_set_name