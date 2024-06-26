"""Sync a directory to iRODS"""

import os
import ssl
import binascii
import base64
import json
import datetime
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
        size_local = os.stat(file_path).st_size
        print(f"irods:{size_irods}, local: {size_local}")
        if size_irods == size_local:
            do_sizes_match = True
        else:
            do_sizes_match = False
            print("Size didn't match")
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

    print(f"Uploading {source} to {destination}.")
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


def list_directory_contents(path):
    """
    List all directories and files under a given path
    """

    directories = []
    files = []

    with os.scandir(path) as entries:
        for entry in entries:
            if entry.is_file():
                files.append(entry.path)
            elif entry.is_dir():
                directories.append(entry.path)
                subdir_directories, subdir_files = list_directory_contents(entry.path)
                directories.extend(subdir_directories)
                files.extend(subdir_files)

    return directories, files



def sync_directory(session, source, destination, logfile, verification_method="size", post_check=False, restartfile= None):
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
    
    restartfile: str
        path to a logfile from the synchronisation script.
        Any files in the 

    Returns
    -------

    results: dict
        A dictionary that contains lists of the files that were skipped, succeeded and failed,
        as well as the total filesize uploaded.
    """

    # adding try-finally so logfile is always written at the end,
    # even when serious errors occur or the user terminates the process.
     

    # For logging
    skipped = []
    succeeded = []
    failed = []
    cumulative_filesize_in_bytes = 0

    directories, files = list_directory_contents(source)
    directories.append(source) # root not in list by default
    # sort directories
    directories.sort(key=lambda x: (x.count('/'), x))

    # If a restartfile is defined, 
    # remove all files in the categories 'succeeded' and
    # 'skipped' from the list 'files. These do not need to be checked/
    # transferred again
    if restartfile:
        print(f"Skipping files mentioned in restartfile {restartfile}")
        with open(restartfile, 'r') as data: 
            restart_info = json.load(data)
            # Adding files that have to be skipped to our 'skipped' list for 
            # this transfer.
            skipped.extend(restart_info['skipped'])
            skipped.extend(restart_info ['succeeded'])
            files = [ item for item in files if not (item in restart_info['skipped'] or item in restart_info ['succeeded']) ]
    try:
        for directory in directories: 
            collection = directory.replace(str(Path(source).parent), destination)
            try:
                session.collections.get(collection)
                print(f"Collection {collection} exists")
            except CollectionDoesNotExist:
                print(f"Creating collection {collection}")
                session.collections.create(collection)
        
        for file in files:
            data_object = file.replace(str(Path(source).parent), destination)
            
            # verification of file, if it exists
            if verification_method == "size":
                files_match = compare_filesize(session, file, data_object)
            elif verification_method == "checksum":
                files_match = compare_checksums(session, file, data_object)

            if not files_match:      
                success = upload_file(session, file, data_object, post_check)
                if success:
                    succeeded.append(file)
                    size = session.data_objects.get(data_object).size
                    cumulative_filesize_in_bytes += size
                else:
                    failed.append(file)
            else: 
                print(f"{data_object} was already uploaded with good status.")
                skipped.append(file)

    finally:
        results = {
            "source": source,
            "destination": destination,
            "succeeded": succeeded,
            "skipped": skipped,
            "failed": failed,
            "cumulative_filesize_in_bytes": cumulative_filesize_in_bytes,
        }
        write_results_to_log(logfile, results)

        return results

def generate_logfile_name():
    """Generate a filename for a logfile"""

    date = datetime.datetime.now()
    formatted_date = date.strftime("%Y%m%d%H%M%S")
    filename = f"sync_log_{formatted_date}.json"

    return filename

def write_results_to_log(filename, results):
    """Write results to a JSON file"""

    with open(filename, "w") as file:
        json.dump(results, file)


def summarize(source, destination, results):
    """Print summary of results"""

    number_skipped = len(results["skipped"])
    number_succeeded = len(results["succeeded"])
    number_failed = len(results["failed"])
    cumulative_filesize_in_bytes = results["cumulative_filesize_in_bytes"]

    print(f"{source} was synchronized to {destination}")
    print(
        f"{number_skipped} files were skipped."
    )
    print(f"{number_succeeded} files were uploaded successfully.")
    print(
        f"Total size of successfully uploaded files is {cumulative_filesize_in_bytes} bytes."
    )
    print(f"{number_failed} files failed to upload or were uploaded incorrectly.")
    print(f"See logfile for more detailed info.")


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
        "--restart-file",
        dest="restart_file",
        nargs='?', 
        const=None, 
        help="""If you want to restart from where a previous transfer left off,
        add the log file as argument. All its succeeded/skipped files will be skipped.""",
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

    with iRODSSession(irods_env_file=env_file, **ssl_settings) as session:

        # generate name for logfile
        logfile = generate_logfile_name()
        # synchronize data to iRODS
        results = sync_directory(
            session, args.source, args.destination, logfile, args.verification, args.post_check, args.restart_file
        )
        # report in standard output
        summarize(args.source, args.destination, results)