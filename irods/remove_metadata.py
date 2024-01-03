"""Remove all metadata from an object"""
import os
import ssl
from argparse import ArgumentParser
from irods.exception import CollectionDoesNotExist, DataObjectDoesNotExist
from irods.meta import iRODSMeta, AVUOperation
from irods.session import iRODSSession
from irods.models import DataObject, DataObjectMeta, Collection, CollectionMeta 
from irods.column import Criterion


def remove_all_avus(Object, prefix=None, verbose=False):
    
    if verbose:
        print(f"Removing metadata from {Object.path}")
    avus_on_Object = Object.metadata.items()
    if prefix == None:
        avus_to_remove = avus_on_Object 
    else:
        avus_to_remove =  [avu for avu in avus_on_Object if avu.name.startswith(prefix)]
    Object.metadata.apply_atomic_operations(
        *[AVUOperation(operation="remove", avu=i) for i in avus_to_remove]
    )

def list_data_objects_with_metadata(session, path, verbose=False):
    query = session.query(DataObjectMeta.name, DataObject.name, Collection.name)
    query.filter(Criterion('like', Collection.name, path + '/%'))
    paths = [f"{result[Collection.name]}/{result[DataObject.name]}" for result in query]
    paths = set(paths) # only unique paths
    if verbose:
        print(f"Found ({len(paths)} data objects with metadata")
    return paths

def list_subcollections_with_metadata(session, path, verbose=False):
    query = session.query(CollectionMeta.name, Collection.name)
    query.filter(Criterion('like', Collection.name, path + '/%'))
    paths = [f"{result[Collection.name]}" for result in query]
    paths = set(paths) # only unique paths
    if verbose:
        print(f"Found ({len(paths)} subcollections with metadata")
    return paths


def main(session, path, prefix = None, recursive=False, verbose=False):
    # if given path is a collection
    try:
        coll = session.collections.get(path)
        remove_all_avus(coll, prefix, verbose)
        if recursive:
            data_objects = list_data_objects_with_metadata(session, path, verbose)
            for data_object in data_objects:
                obj = session.data_objects.get(data_object)
                remove_all_avus(obj, prefix, verbose)
            subcollections = list_subcollections_with_metadata(session, path, verbose)
            for subcollection in subcollections:
                obj = session.collections.get(subcollection)
                remove_all_avus(obj, prefix, verbose)
    except CollectionDoesNotExist:
        # if given path is a data object
        try:
            obj = session.data_objects.get(path)
            if recursive:
                raise Exception(
                    f"You cannot use a recursive operation on a data object."
                )
            remove_all_avus(obj, prefix, verbose)
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
    parser.add_argument(
        "-t",
        dest="test",
        action="store_true",
        help="test",
    )
    parser.add_argument('--prefix', dest='prefix', nargs='?', default=None)
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
        main(session, args.path, args.prefix, args.recursive, args.verbose)
