"""Remove all metadata from an object"""
import os
import ssl
from argparse import ArgumentParser
from irods.exception import CollectionDoesNotExist, DataObjectDoesNotExist
from irods.meta import iRODSMeta, AVUOperation
from irods.session import iRODSSession


def remove_all_avus(Object):
    # taken from examples at https://github.com/irods/python-irodsclient

    avus_on_Object = Object.metadata.items()
    Object.metadata.apply_atomic_operations( *[AVUOperation(operation='remove', avu=i) for i in avus_on_Object] )


def main(session, path):
    
    try:
        coll = session.collections.get(path)
        remove_all_avus(coll)
    except CollectionDoesNotExist:
        try: 
            obj = session.data_objects.get(path)
            remove_all_avus(obj)
        except DataObjectDoesNotExist:
            raise Exception(f"{path} is not recognized as object or collection in iRODS")

        
if __name__ == "__main__":
    # Handling command-line arguments
    parser = ArgumentParser(usage=__doc__)
    parser.add_argument(dest="path", help="The path of the data object or collection")
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
        main(session, args.path)
