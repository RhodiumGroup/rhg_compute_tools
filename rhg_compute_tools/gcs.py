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


def _get_path_types(src,dest):
    src_gs = src
    dest_gs = dest
    dest_gcs = dest
    if src_gs.startswith('/gcs/'):
        src_gs = src.replace('/gcs/','gs://')
    if dest_gs.startswith('/gcs/'):
        dest_gs = dest.replace('/gcs/', 'gs://')
    if dest_gcs.startswith('gs://'):
        dest_gcs = dest.replace('gs://', '/gcs/')  
        
    return src_gs, dest_gs, dest_gcs


def cp_to_gcs(*args, **kwargs):
    '''Deprecated. Use `cp_gcs`.'''
    return cp_gcs(*args, **kwargs)
    
def cp_gcs(src, dest, cp_flags=[]):
    '''Copy a file or recursively copy a directory from local
    path to GCS or vice versa. Must have already authenticated to use.
    Notebook servers are automatically authenticated, but workers 
    need to pass the path to the authentication json file to the 
    GCLOUD_DEFAULT_TOKEN_FILE env var.
    This is done automatically for rhg-data.json when using the get_worker
    wrapper.

    Parameters
    ----------
    src, dest : str
        The paths to the source and destination file or directory. 
        If on GCS, either the `/gcs` or `gs:/` prefix will work.
    cp_flags : list of str, optional
        String of flags to add to the gsutil cp command. e.g. 
        `cp_flags=['r']` will run the command `gsutil -m cp -r...`
        (recursive copy)

    Returns
    -------
    str
        stdout from gsutil call
    str
        stderr from gsutil call
    :py:class:`datetime.timedelta`
        Time it took to copy file(s).
    '''

    st_time = dt.now()

    # make sure we're using URL
    # if /gcs or gs:/ are not in src or not in dest
    # then these won't change anything
    src_gs, dest_gs, dest_gcs = _get_path_types(src,dest)
    
    # if directory already existed cp would put src into dest_gcs
    if exists(dest_gcs):
        dest_base = join(dest_gcs, basename(src))
    # else cp would have put the contents of src into the new directory
    else:
        dest_base = dest_gcs

    cmd = 'gsutil -m cp ' + ' '.join(['-'+f for f in cp_flags]) + ' {} {}'.format(src_gs, dest_gs)
    cmd = shlex.split(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    # need to add directories if you were recursively copying a directory
    if isdir(src) and dest_gcs.startswith('/gcs/'):
        # now make directory blobs on gcs so that gcsfuse recognizes it
        dirs_to_make = [x[0].replace(src, dest_base) for x in os.walk(src)]
        for d in dirs_to_make:
            os.makedirs(d, exist_ok=True)

    end_time = dt.now()

    return stdout, stderr, end_time - st_time

def sync_to_gcs(*args, **kwargs):
    '''Deprecated. Use sync_gcs'''
    return sync_gcs(*args, **kwargs)

def sync_gcs(src, dest, sync_flags=['r','d']):
    '''Sync a directory from local to GCS or vice versa. Uses `gsutil rsync`.
    Must have already authenticated to use. Notebook servers
    are automatically authenticated, but workers need to pass the path
    to the authentication json file to the GCLOUD_DEFAULT_TOKEN_FILE env var.
    This is done automatically for rhg-data.json when using the get_worker
    wrapper.

    Parameters
    ----------
    src, dest : str
        The paths to the source and destination file or directory. 
        If on GCS, either the `/gcs` or `gs:/` prefix will work.
    sync_flags : list of str, optional
        String of flags to add to the gsutil cp command. e.g. 
        `sync_flags=['r','d']` will run the command `gsutil -m cp -r -d...`
        (recursive copy, delete any files on dest that are not on src).
        This is the default set of flags.

    Returns
    -------
    str
        stdout from gsutil call
    str
        stderr from gsutil call
    :py:class:`datetime.timedelta`
        Time it took to copy file(s).
    '''

    st_time = dt.now()

    # remove trailing /'s
    src = src.rstrip('/')
    dest = dest.rstrip('/')
    
    # make sure we're using URL
    # if /gcs or gs:/ are not in src or not in dest
    # then these won't change anything
    src_gs, dest_gs, dest_gcs = _get_path_types(src,dest)

    cmd = 'gsutil -m rsync ' + ' '.join(['-'+f for f in sync_flags]) + ' {} {}'.format(src_gs, dest_gs)
    cmd = shlex.split(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    # need to add directories if you were recursively copying a directory TO gcs
    # now make directory blobs on gcs so that gcsfuse recognizes it
    if dest_gcs.startswith('/gcs/'):
        dirs_to_make = [x[0].replace(src, dest_gcs) for x in os.walk(src)]
        for d in dirs_to_make:
            os.makedirs(d, exist_ok=True)

    end_time = dt.now()

    return stdout, stderr, end_time - st_time