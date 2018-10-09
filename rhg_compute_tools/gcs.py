# -*- coding: utf-8 -*-

"""Tools for interacting with GCS infrastructure."""

import os
from os.path import join, isdir, basename, exists
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime as dt
import subprocess
import shlex


def get_bucket(cred_path):
    '''Return a bucket object from Rhg's GCS system.

    Parameters
    ----------
    cred_path : str
        Path to credentials file. Default is the default location on RHG
        workers.

    Returns
    -------
    bucket : :py:class:`google.cloud.storage.bucket.Bucket`
    '''

    credentials = service_account.Credentials.from_service_account_file(
        cred_path)
    sclient = storage.Client(credentials=credentials)
    bucket = sclient.get_bucket('rhg-data')

    return bucket


def cp_to_gcs(src, dest):
    '''Copy a file or recursively copy a directory from local
    path to GCS. Must have already authenticated to use. Notebook servers
    are automatically authenticated, but workers need to pass the path
    to the authentication json file to the GCLOUD_DEFAULT_TOKEN_FILE env var.
    This is done automatically for rhg-data.json when using the get_worker
    wrapper.

    Parameters
    ----------
    src : str
        The local path to either a file (single copy) or a directory (recursive
        copy).
    dest : str
        If copying a directory, this is the path of the directory blob on GCS.
        If it does not exist, it will be created and populated with the
        contents of src. If it does exist, basename(src) will be placed inside
        dest. If copying a file, this is the path of the file blob on GCS. This
        path can begin with either '/gcs/rhg-data' or 'gs://rhg-data'.
        There is no difference in behavior.

    Returns
    -------
    :py:class:`datetime.timedelta`
        Time it took to copy file(s).
    '''

    st_time = dt.now()

    # construct cp command
    if dest[0] == '/':
        dest_gs = dest.replace('/gcs/', 'gs://')
        dest_gcs = dest
    elif dest[0] == 'g':
        dest_gs = dest
        dest_gcs = dest.replace('gs://', '/gcs/')

    # if directory already existed cp would put src into dest_gcs
    if exists(dest_gcs):
        dest_base = join(dest_gcs, basename(src))
    # else cp would have put the contents of src into the new directory
    else:
        dest_base = dest_gcs

    cmd = 'gsutil '
    if isdir(src):
        cmd += '-m cp -r '
    cmd += '{} {}'.format(src, dest_gs)
    cmd = shlex.split(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    # need to add directories if you were recursively copying a directory
    if isdir(src):
        # now make directory blobs on gcs so that gcsfuse recognizes it
        dirs_to_make = [x[0].replace(src, dest_base) for x in os.walk(src)]
        for d in dirs_to_make:
            os.makedirs(d, exist_ok=True)

    end_time = dt.now()

    return stdout, stderr, end_time - st_time
