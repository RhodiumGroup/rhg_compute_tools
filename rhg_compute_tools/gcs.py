# -*- coding: utf-8 -*-

"""Tools for interacting with GCS infrastructure."""

import os
import shlex
import subprocess
from datetime import datetime as dt
from os.path import basename, exists, isdir, join

from google.cloud import storage
from google.oauth2 import service_account


def authenticated_client(credentials=None, **client_kwargs):
    """Convenience function to create an authenticated GCS client.

    Parameters
    ----------
    credentials : str or None, optional
        Str path to storage credentials authentication file. If None
        is passed (default) will create a Client object with no args, using
        the authorization credentials for the current environment. See the
        [google cloud storage docs](
        https://googleapis.dev/python/google-api-core/latest/auth.html)
        for an overview of the authorization options.
    client_kwargs : optional
        kwargs to pass to the `get_client` function

    Returns
    -------
    google.cloud.storage.Client
    """
    if credentials is None:
        client = storage.Client()
    else:
        creds = service_account.Credentials.from_service_account_file(str(credentials))
        client = storage.Client(credentials=creds, **client_kwargs)

    return client


def get_bucket(
    credentials=None, bucket_name="rhg-data", return_client=False, **client_kwargs
):
    """Return a bucket object from Rhg's GCS system.

    Parameters
    ----------
    credentials : str or None, optional
        Str path to storage credentials authentication file. If None
        is passed (default) will create a Client object with no args, using
        the authorization credentials for the current environment. See the
        [google cloud storage docs](
        https://googleapis.dev/python/google-api-core/latest/auth.html)
        for an overview of the authorization options.
    bucket_name : str, optional
        Name of bucket. Typically, we work with ``rhg_data`` (default)
    return_client : bool, optional
        Return the Client object as a second object.
    client_kwargs : optional
        kwargs to pass to the `get_client` function

    Returns
    -------
    bucket : :py:class:`google.cloud.storage.bucket.Bucket`
    """
    client = authenticated_client(credentials=credentials, **client_kwargs)
    result = client.get_bucket(bucket_name)

    if return_client:
        result = (result, client)

    return result


def _remove_prefix(text, prefix="/gcs/rhg-data/"):
    return text[text.startswith(prefix) and len(prefix) :]


def _get_path_types(src, dest):
    src_gs = src
    src_gcs = src
    dest_gs = dest
    dest_gcs = dest
    if src_gs.startswith("/gcs/"):
        src_gs = src.replace("/gcs/", "gs://")
    if src_gcs.startswith("gs://"):
        src_gs = src.replace("gs://", "/gcs/")
    if dest_gs.startswith("/gcs/"):
        dest_gs = dest.replace("/gcs/", "gs://")
    if dest_gcs.startswith("gs://"):
        dest_gcs = dest.replace("gs://", "/gcs/")

    return src_gs, src_gcs, dest_gs, dest_gcs


def rm(path, flags=[]):
    """Remove a file or recursively remove a directory from local
    path to GCS or vice versa. Must have already authenticated to use.
    Notebook servers are automatically authenticated, but workers
    need to pass the path to the authentication json file to the
    GOOGLE_APPLICATION_CREDENTIALS env var.
    This is done automatically for rhg-data.json when using the get_worker
    wrapper.

    Parameters
    ----------
    path : str or :class:`pathlib.Path`
        The path to the source and destination file or directory.
        Either the `/gcs` or `gs:/` prefix will work.
    flags : list of str, optional
        String of flags to add to the gsutil rm command. e.g.
        `flags=['r']` will run the command `gsutil -m rm -r...`
        (recursive remove)

    Returns
    -------
    str
        stdout from gsutil call
    str
        stderr from gsutil call
    :py:class:`datetime.timedelta`
        Time it took to copy file(s).
    """

    st_time = dt.now()

    path = str(path).replace("/gcs/", "gs://")

    cmd = "gsutil -m rm " + " ".join(["-" + f for f in flags]) + f" {path}"

    print(f"Running cmd: {cmd}")
    cmd = shlex.split(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    end_time = dt.now()

    return stdout, stderr, end_time - st_time


def replicate_directory_structure_on_gcs(src, dst, client):
    """
    Replicate a local directory structure on google cloud storage

    Parameters
    ----------
    src : str
        Path to the root directory on the source machine. The directory
        structure within this directory will be reproduced within `dst`,
        e.g. `/Users/myusername/my/data`
    dst : str
        A url for the root directory of the destination, starting with
        `gs://[bucket_name]/`, e.g. `gs://my_bucket/path/to/my/data`
    client : google.cloud.storage.client.Client
        An authenticated :py:class:`google.cloud.storage.client.Client` object.
    """
    if dst.startswith("gs://"):
        dst = dst[5:]
    elif dst.startswith("gcs://"):
        dst = dst[6:]
    else:
        raise ValueError("dst must begin with `gs://` or `gcs://`")

    bucket_name = dst.split("/")[0]
    blob_path = "/".join(dst.split("/")[1:])

    bucket = client.get_bucket(bucket_name)

    for d, dirnames, files in os.walk(src):
        dest_path = os.path.join(blob_path, os.path.relpath(d, src))

        # make sure there is exactly one trailing slash:
        dest_path = dest_path.rstrip("/") + "/"

        # ignore "." directory
        if dest_path == "./":
            continue

        blob = bucket.blob(dest_path)
        blob.upload_from_string("")


def cp(src, dest, flags=[]):
    """Copy a file or recursively copy a directory from local
    path to GCS or vice versa. Must have already authenticated to use.
    Notebook servers are automatically authenticated, but workers
    need to pass the path to the authentication json file to the
    GOOGLE_APPLICATION_CREDENTIALS env var.
    This is done automatically for rhg-data.json when using the get_worker
    wrapper.

    Parameters
    ----------
    src, dest : str
        The paths to the source and destination file or directory.
        If on GCS, either the `/gcs` or `gs:/` prefix will work.
    flags : list of str, optional
        String of flags to add to the gsutil cp command. e.g.
        `flags=['r']` will run the command `gsutil -m cp -r...`
        (recursive copy)

    Returns
    -------
    str
        stdout from gsutil call
    str
        stderr from gsutil call
    :py:class:`datetime.timedelta`
        Time it took to copy file(s).
    """

    st_time = dt.now()

    src = str(src)
    dest = str(dest)

    # make sure we're using URL
    # if /gcs or gs:/ are not in src or not in dest
    # then these won't change anything
    src_gs, src_gcs, dest_gs, dest_gcs = _get_path_types(src, dest)

    # if directory already existed cp would put src into dest_gcs
    if exists(dest_gcs):
        dest_base = join(dest_gcs, basename(src))
    # else cp would have put the contents of src into the new directory
    else:
        dest_base = dest_gcs

    cmd = (
        "gsutil -m cp "
        + " ".join(["-" + f for f in flags])
        + " {} {}".format(src_gs, dest_gs)
    )

    print(f"Running cmd: {cmd}")
    cmd = shlex.split(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    # need to add directories if you were recursively copying a directory
    if isdir(src_gcs) and dest_gcs.startswith("/gcs/"):
        # now make directory blobs on gcs so that gcsfuse recognizes it
        dirs_to_make = [x[0].replace(src, dest_base) for x in os.walk(src)]
        for d in dirs_to_make:
            os.makedirs(d, exist_ok=True)

    end_time = dt.now()

    return stdout, stderr, end_time - st_time


def sync(src, dest, flags=["r", "d"]):
    """Sync a directory from local to GCS or vice versa. Uses `gsutil rsync`.
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
    flags : list of str, optional
        String of flags to add to the gsutil cp command. e.g.
        `flags=['r','d']` will run the command `gsutil -m cp -r -d...`
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
    """

    st_time = dt.now()

    src = str(src)
    dest = str(dest)

    # remove trailing /'s
    src = src.rstrip("/")
    dest = dest.rstrip("/")

    # make sure we're using URL
    # if /gcs or gs:/ are not in src or not in dest
    # then these won't change anything
    src_gs, src_gcs, dest_gs, dest_gcs = _get_path_types(src, dest)

    cmd = (
        "gsutil -m rsync "
        + " ".join(["-" + f for f in flags])
        + " {} {}".format(src_gs, dest_gs)
    )

    print(f"Running cmd: {cmd}")

    cmd = shlex.split(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    # need to add directories if you were recursively copying a directory TO
    # gcs now make directory blobs on gcs so that gcsfuse recognizes it
    if dest_gcs.startswith("/gcs/"):
        dirs_to_make = [x[0].replace(src_gcs, dest_gcs) for x in os.walk(src_gcs)]
        for d in dirs_to_make:
            os.makedirs(d, exist_ok=True)

    end_time = dt.now()

    return stdout, stderr, end_time - st_time


def ls(dir_path):
    """List a directory quickly using `gsutil`"""

    dir_url = str(dir_path).replace("/gcs/", "gs://")

    cmd = f"gsutil ls {dir_url}"

    print(f"Running cmd: {cmd}")
    cmd = shlex.split(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    res = [x.split("/")[1].rstrip(r"\\n") for x in str(stdout).split(dir_url)[2:]]

    return res
