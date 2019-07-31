# -*- coding: utf-8 -*-
from . import __version__
from intake.source.base import DataSource, Schema

import json
import dask.dataframe as dd
from datetime import datetime, timedelta
from azureblobfs.dask import DaskAzureBlobFileSystem


class AzureSource(DataSource):
    """Common behaviours for plugins in this repo"""
    name = 'azure_source'
    version = __version__
    container = 'dataframe'
    partition_access = True

    def __init__(self, blob_uri, storage_account_name, access_key, azure_blob_prefix='abfs://', kwargs=None):
        """
        Parameters
        ----------
        blob_uri : str
            The Azure Blob URI.
        storage_account_name : str
            Azure storage account name.
        access_key : str
            Access key to authorize access to the Azure storage account.
        azure_blob_prefix: str
            The prefix for accessing Azure Blob Storage. Defaults to the `abfs://` protocol.
        """
        self._blob_uri = blob_uri
        self._storage_account_name = storage_account_name
        self._access_key = access_key
        self._azure_blob_prefix = azure_blob_prefix
        self._dataframe = None

    def _open_dataset(self):
        df = dd.read_csv(self._blob_uri, storage_options={"account_name": self._storage_account_name, "account_key": self._access_key}, blocksize=None)

        self._dataframe = df

    def _get_schema(self):
        if self._dataframe is None:
            self._open_dataset()

        dtypes = self._dataframe._meta.dtypes.to_dict()
        dtypes = {n: str(t) for (n, t) in dtypes.items()}
        return Schema(datashape=None,
                      dtype=dtypes,
                      shape=(None, len(dtypes)),
                      npartitions=self._dataframe.npartitions,
                      extra_metadata={})

    def _get_partition(self, i):
        self._get_schema()
        return self._dataframe.get_partition(i).compute()

    def read(self):
        self._get_schema()
        return self._dataframe.compute()

    def to_dask(self):
        self._get_schema()
        return self._dataframe

    def _close(self):
        self._dataframe = None
