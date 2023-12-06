"""Sync a directory to iRODS"""

import os
import ssl
from pathlib import Path
from irods.session import iRODSSession
from argparse import ArgumentParser


def sync_directory(session, source, destination):
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
    """

    # Create a collection for the current directory
    directory = Path(source)
    collection = f"{destination}/{directory.name}"
    print(f"Creating collection {collection}")
    session.collections.create(collection)

    # upload all files in the directory
    files = [f for f in directory.iterdir() if f.is_file()]
    for file in files:
        data_object = f"{collection}/{file.name}"

        # Check if the object exists in iRODS,
        # and has the same size.
        # If so, upload the file.
        should_sync = True
        try:
            size_irods = session.data_objects.get(data_object).size
            size_local = file.stat().st_size
            if (size_irods == size_local):
                should_sync = False
        except Exception:
            should_sync = True
        if should_sync:
            print(f"Uploading {file}.")
            session.data_objects.put(file, data_object)
        else:
            print(f"{data_object} was already uploaded with correct size ({size_irods} bytes).")

    # for all subdirectories, run this function again
    subdirs = [d for d in directory.iterdir() if d.is_dir()]
    for subdir in subdirs:
        sync_directory(session, subdir, collection)


if __name__ == '__main__':

    # get command-line arguments
    parser = ArgumentParser(usage=__doc__)
    parser.add_argument(dest='source',
                        help="The path of the directory you want to upload")
    parser.add_argument(dest='destination',
                        help="The destination in iRODS")
    args = parser.parse_args()
    source = args.source
    destination = args.destination

    # Create an iRODS session
    try:
        env_file = os.environ['IRODS_ENVIRONMENT_FILE']
    except KeyError:
        env_file = os.path.expanduser('~/.irods/irods_environment.json')
    ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH,
                                             cafile=None,
                                             capath=None,
                                             cadata=None)
    ssl_settings = {'ssl_context': ssl_context}

    with iRODSSession(irods_env_file=env_file, **ssl_settings) as session:
        sync_directory(session, source, destination)
