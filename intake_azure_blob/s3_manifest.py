# -*- coding: utf-8 -*-
#from . import __version__
from intake.source.base import DataSource, Schema

import json
import dask.dataframe as dd
from datetime import datetime, timedelta
#import s3fs

from azure.storage.blob import BlockBlobService
import pandas as pd
from io import StringIO


class AzureSource(DataSource):
    """Common behaviours for plugins in this repo"""
    name = 'azure_source'
#    version = __version__
    container = 'dataframe'
    partition_access = True

    def __init__(self, blob_name, container_name, storage_account_name, access_key, azure_blob_prefix='blob:', kwargs=None, metadata=None):
        """
        Parameters
        ----------
        blob_name : str
            Name of the Blob.
        container_name : str
            Azure Blob container name.
        storage_account_name : str
            Azure storage account name.
        access_key: str
            Access key to authorize access to the Azure storage account.
        azure_blob_prefix: str
            The prefix for accessing Azure Blob Storage. Defaults to the `blob:` protocol.
        kwargs : dict
            Any further arguments to pass to Dask's read_csv (such as block size)
            or to the `CSV parser <https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html>`_
            in pandas (such as which columns to use, encoding, data-types)
        """
        self._blob_name = blob_name
        self._container_name = container_name
        self._storage_account_name = storage_account_name
        self._access_key = access_key
        self._azure_blob_prefix = azure_blob_prefix
        self._dataframe = None

    def _open_dataset(self):

        blob_service = BlockBlobService(account_name=self._storage_account_name, account_key=self._access_key)
        blobstring = blob_service.get_blob_to_text(self._container_name, self._blob_name).content
        df = pd.read_csv(StringIO(blobstring))
        #df = dd.read_csv(StringIO(blobstring)) 
        self._dataframe = df

    def _get_schema(self):
        if self._dataframe is None:
            self._open_dataset()

#        dtypes = self._dataframe._meta.dtypes.to_dict()
        dtypes = self._dataframe.dtypes.to_dict()
        dtypes = {n: str(t) for (n, t) in dtypes.items()}
        return Schema(datashape=None,
                      dtype=dtypes,
                      shape=(None, len(dtypes)),
                      extra_metadata={})

    def _get_partition(self, i):
        self._get_schema()
        return self._dataframe.get_partition(i).compute()

    def read(self):
        self._get_schema()
        return self._dataframe

    def to_dask(self):
        self._get_schema()
        return self._dataframe

    def _close(self):
        self._dataframe = None
