"""Upload a directory to iRODS"""

import os
import ssl
from pathlib import Path
from irods.session import iRODSSession
from argparse import ArgumentParser


def upload_directory(session, source, destination):
    """
    Upload a directory to iRODS

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

    # create a collection for the current directory
    directory = Path(source)
    collection = f"{destination}/{directory.name}"
    session.collections.create(collection)

    # upload all files in the directory
    files = [f for f in directory.iterdir() if f.is_file()]
    for file in files:
        data_object = f"{collection}/{file.name}"
        session.data_objects.put(file, data_object)

    # for all subdirectories, run this function again
    subdirs = [d for d in directory.iterdir() if d.is_dir()]
    for subdir in subdirs:
        upload_directory(session, subdir, collection)


if __name__ == '__main__':

    # get command-line arguments
    parser = ArgumentParser(usage=__doc__)
    parser.add_argument(dest='source',
                        help="The path of the directory you want to upload")
    parser.add_argument(dest='destination',
                        help="The destination in ManGO")
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

        upload_directory(session, source, destination)
