"""Write metadata on an object to CSV"""

import os
from pathlib import Path
import ssl
import csv
from argparse import ArgumentParser
from irods.session import iRODSSession


def main(session, path):
    obj = session.data_objects.get(path)
    do_name = obj.name
    print(f"Downloading metadata for {do_name}")
    metadata = obj.metadata.items()

    do_without_extension = Path(path).stem
    csv_filename = f"{do_without_extension}_metadata_reworked.csv"

    with open(csv_filename, "w") as file:
        writer = csv.writer(file)
        header = ["attribute","value","units"]
        writer.writerow(header)
        for avu in metadata:
            data = [avu.name, avu.value, avu.units]
            writer.writerow(data)
            


if __name__ == "__main__":
    # Handling commandline arguments
    parser = ArgumentParser(usage=__doc__)
    parser.add_argument(dest="path", help="The path to the data object")
    args = parser.parse_args()

    try:
        env_file = os.environ["IRODS_ENVIRONMENT_FILE"]
    except KeyError:
        env_file = os.path.expanduser("~/.irods/irods_environment_prc.json")
    with iRODSSession(irods_env_file=env_file) as session:
        main(session, args.path)
