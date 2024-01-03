"""Remove all metadata from an object"""
import os
import ssl
from argparse import ArgumentParser
from irods.exception import CollectionDoesNotExist, DataObjectDoesNotExist
from irods.meta import iRODSMeta, AVUOperation
from irods.session import iRODSSession


def remove_all_avus(Object, verbose=False):
    
    if verbose:
        print(f"Removing metadata from {Object.path}")
    # taken from examples at https://github.com/irods/python-irodsclient
    avus_on_Object = Object.metadata.items()
    Object.metadata.apply_atomic_operations(
        *[AVUOperation(operation="remove", avu=i) for i in avus_on_Object]
    )


def main(session, path, recursive=False, verbose=False):
    # if given path is a collection
    try:
        coll = session.collections.get(path)
        remove_all_avus(coll, verbose)
        if recursive:
            data_objects = coll.data_objects
            for obj in data_objects:
                remove_all_avus(obj, verbose)
            subcollections = coll.subcollections
            for subcollection in subcollections:
                main(session, subcollection.path, recursive, verbose)
    except CollectionDoesNotExist:
        # if given path is a data object
        try:
            obj = session.data_objects.get(path)
            if recursive:
                raise Exception(
                    f"You cannot use a recursive operation on a data object."
                )
            remove_all_avus(obj, verbose)
        except DataObjectDoesNotExist:
            # if given path doesn't exist
            raise Exception(
                f"{path} is not recognized as object or collection in iRODS"
            )


if __name__ == "__main__":
    # Handling command-line arguments
    parser = ArgumentParser(usage=__doc__)
    parser.add_argument(dest="path", help="The path of the data object or collection")
    parser.add_argument(
        "-r",
        dest="recursive",
        action="store_true",
        help="Remove metadata from a collection and all its contents, including subcollections",
    )
    parser.add_argument(
        "-v",
        dest="verbose",
        action="store_true",
        help="Verbose mode",
    )
    args = parser.parse_args()

    # creating iRODS session
    try:
        env_file = os.environ["IRODS_ENVIRONMENT_FILE"]
    except KeyError:
        env_file = os.path.expanduser("~/.irods/irods_environment_prc.json")

    ssl_context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH, cafile=None, capath=None, cadata=None
    )
    ssl_settings = {"ssl_context": ssl_context}
    with iRODSSession(irods_env_file=env_file, **ssl_settings) as session:
        main(session, args.path, args.recursive, args.verbose)
