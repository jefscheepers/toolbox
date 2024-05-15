"""Sync a directory to iRODS"""

import os
import ssl
import binascii
import base64
from hashlib import sha256
from pathlib import Path
from irods.session import iRODSSession
from irods.exception import CollectionDoesNotExist, DataObjectDoesNotExist
from argparse import ArgumentParser


def compare_filesize(session, file_path, data_object_path):
    """
    Compare the size of a local file and a file in iRODS

    The function returns True in case both sizes match.
    It returns False in case the sizes don't match,
    or the data object doesn't exist

    Arguments
    ---------
    session: obj
        An iRODSSession object

    local_path: str
        The path of a local file
        Please provide a full path

    destination: str
        The path to a data object in iRODS
        Please provide a full path.

    Returns
    -------

    do_sizes_match: bool
        True if sizes match, False in case of a mismatch.
    """

    try:
        size_irods = session.data_objects.get(data_object_path).size
        size_local = file_path.stat().st_size
        if size_irods == size_local:
            do_sizes_match = True
        else:
            do_sizes_match = False
    except (DataObjectDoesNotExist, CollectionDoesNotExist):
        # Function will fail if data object doesn't exist
        do_sizes_match = False
    return do_sizes_match


def irods_to_sha256_checksum(irods_checksum):
    """Transforms a checksum from iRODS to the standard sha256 checksum"""

    if irods_checksum is None or not irods_checksum.startswith("sha2:"):
        return None

    sha256_checksum = binascii.hexlify(base64.b64decode(irods_checksum[5:]))
    sha256_checksum = sha256_checksum.decode("utf-8")

    return sha256_checksum


def compare_checksums(session, file_path, data_object_path):
    """Check whether the checksum of a local file matches its iRODS equivalent


    Arguments
    ---------
    session: obj
        An iRODSSession object

    file_path: str
        The path of a local file
        Please provide a full path

    data_object_path: str
        The path to a data object in iRODS
        Please provide a full path.

    Returns
    -------

    do_checksums_match: bool
        True if sizes match, False in case of a mismatch.
    """

    try:
        # get checksum from iRODS
        # put first so function fails early if data object does not exist
        obj = session.data_objects.get(data_object_path)
        irods_checksum = obj.chksum()
        irods_checksum_sha256 = irods_to_sha256_checksum(irods_checksum)

        # get local checksum
        hash_sha256 = sha256()
        with open(file_path, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hash_sha256.update(chunk)
        local_checksum_sha256 = hash_sha256.hexdigest()

        do_checksums_match = local_checksum_sha256 == irods_checksum_sha256
    except (CollectionDoesNotExist, DataObjectDoesNotExist):
        # Function will fail if data object doesn't exist
        do_checksums_match = False

    return do_checksums_match


def upload_file(session, source, destination, post_check=False):
    """Upload a file to iRODS

    Arguments
    ---------

    session: obj
        An iRODSSession object

    source: str
        The path of a local file
        Please provide a full path

    destination: str
        The path to a data object in iRODS
        Please provide a full path.

    post_check: bool
        Whether to checksum files after upload

    Returns
    -------
    success: bool
        True if file was successfully uploaded, otherwise false
    """

    print(f"Uploading {source}.")
    try:
        session.data_objects.put(source, destination)
        if post_check:
            print("Verifying file after transfer")
            success = compare_checksums(session, source, destination)
        else:
            success = True

    except:
        print(f"Uploading {source} failed")
        success = False
    return success


def sync_directory(
    session, source, destination, verification_method="size", post_check=False
):
    """
    Synchronize a directory to iRODS

    Arguments
    ---------

    session: obj
        An iRODSSession object

    source: str
        The path of a local directory.
        Please provide a full path

    destination: str
        The path to where you want to upload
        the directory.
        Please provide a full path.

    verification_method: str
        Method of verifying whether a file in iRODS should be updated
        Options:
            - size
            - checksum

    post_check: bool
        Whether to checksum files after upload

    Returns
    -------

    results: dict
        A dictionary that contains lists of the files that were skipped, succeeded and failed.
    """

    succeeded = []
    skipped = []
    failed = []

    # Create a collection for the current directory
    directory = Path(source)
    collection = f"{destination}/{directory.name}"
    try:
        session.collections.get(collections)
    except CollectionDoesNotExist:
        print(f"Creating collection {collection}")
        session.collections.create(collection)

    # upload all files in the directory
    files = [f for f in directory.iterdir() if f.is_file()]
    for file in files:
        data_object = f"{collection}/{file.name}"

        # verification of file, if it exists
        if verification_method == "size":
            files_match = compare_filesize(session, file, data_object)
        elif verification_method == "checksum":
            files_match = compare_checksums(session, file, data_object)

        if not files_match:
            success = upload_file(session, file, data_object, post_check)
            if success:
                succeeded.append(data_object)
            else:
                failed.append(file)
        else:
            print(f"{data_object} was already uploaded with good status.")
            skipped.append(data_object)

    results = {"succeeded": succeeded, "skipped": skipped, "failed": failed}

    # for all subdirectories, run this function again
    subdirs = [d for d in directory.iterdir() if d.is_dir()]
    for subdir in subdirs:
        subdir_result = sync_directory(
            session, subdir, collection, verification_method, post_check
        )
        results["succeeded"].extend(subdir_result["succeeded"])
        results["skipped"].extend(subdir_result["skipped"])
        results["failed"].extend(subdir_result["failed"])

    return results


if __name__ == "__main__":
    # get command-line arguments
    parser = ArgumentParser(usage=__doc__)
    parser.add_argument(
        "--verification",
        dest="verification",
        default="size",
        nargs="?",
        help="The method of verification of files (size/checksum)",
    )
    parser.add_argument(
        "--post-check",
        dest="post_check",
        action="store_true",
        help="Check checksum after upload to verify whether the file(s) are uploaded correctly",
    )
    parser.add_argument(
        dest="source", help="The path of the directory you want to upload"
    )
    parser.add_argument(dest="destination", help="The destination in iRODS")
    args = parser.parse_args()

    # Create an iRODS session
    try:
        env_file = os.environ["IRODS_ENVIRONMENT_FILE"]
    except KeyError:
        env_file = os.path.expanduser("~/.irods/irods_environment.json")
    ssl_context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH, cafile=None, capath=None, cadata=None
    )
    ssl_settings = {"ssl_context": ssl_context}

    print(args.post_check)
    with iRODSSession(irods_env_file=env_file, **ssl_settings) as session:
        results = sync_directory(
            session, args.source, args.destination, args.verification, args.post_check
        )
        print(results)
