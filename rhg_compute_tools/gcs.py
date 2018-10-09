# -*- coding: utf-8 -*-

"""Tools for interacting with GCS infrastructure."""

from os.path import join, isfile, basename
from google.cloud import storage
from google.oauth2 import service_account
from glob import glob
from datetime import datetime as dt


def get_bucket(cred_path):
    '''Return a bucket object from Rhg's GCS system.

    Parameters
    ----------
    cred_path : str
        Path to credentials file. Default is the default location on RHG
        workers.

    Returns
    -------
    bucket : :py:class:`google.cloud.storage.Bucket`
    '''

    credentials = service_account.Credentials.from_service_account_file(
        cred_path)
    sclient = storage.Client(credentials=credentials)
    bucket = sclient.get_bucket('rhg-data')

    return bucket


def _cp_dir_to_gcs(bucket, src, dest):
    '''Recursively copy a directory from local path to GCS'''

    files_loc = glob(join(src, '*'))
    if src[-1] != '/':
        src += '/'

    # upload directory blob
    if bucket.get_blob(dest) is None:
        newblob = bucket.blob(dest)
        newblob.upload_from_string('')

    # upload file blobs
    for f_loc in files_loc:
        fname = basename(f_loc)
        this_dest = join(dest, fname)
        if isfile(f_loc):
            newblob = bucket.blob(this_dest)
            newblob.upload_from_filename(f_loc)
        # if directory, call this recursively
        else:
            _cp_dir_to_gcs(bucket, f_loc, this_dest)


def cp_to_gcs(src, dest, cred_path='/opt/gcsfuse_tokens/rhg-data.json'):
    '''Copy a file or recursively copy a directory from local
    path to GCS.

    Parameters
    ----------
    src : str
        The local path to either a file (single copy) or a directory (recursive
        copy).
    dest : str
        If copying a directory, this is the path of the directory blob on GCS.
        If copying a file, this is the path of the file blob on GCS.
        This path begins with e.g. 'impactlab-rhg/...'.
    cred_path (optional) : str
        Path to credentials file. Default is the default location on RHG
        workers.

    Returns
    -------
    :py:class:`datetime.timedelta`
        Time it took to copy file(s).
    '''

    st_time = dt.now()

    bucket = get_bucket(cred_path)

    if isfile(src):
        newblob = bucket.blob(dest)
        newblob.upload_from_filename(src)

    else:
        _cp_dir_to_gcs(bucket, src, dest)

    return dt.now() - st_time
