import netCDF4 as nc
import csv
from pathlib import Path
from argparse import ArgumentParser

def main(file):

    rootgrp = nc.Dataset(file)
    metadata = []
    for i in rootgrp.ncattrs(): 
        metadata.append((i, getattr(rootgrp, i), 'ncattrs'))
    for i in rootgrp.dimensions:
        metadata.append((i, rootgrp.dimensions[i].size, 'dimensions'))
    for i in rootgrp.variables:
        metadata.append((i, rootgrp.variables[i].long_name, 'variables'))


    filename_without_extension = Path(file).stem
    csv_filename = f"{filename_without_extension}_metadata.csv"

    with open(csv_filename, "w") as file:
        file.write("Attribute,value,units\n")
        for element in metadata:
            file.write(f"{element[0]},{element[1]},{element[2]}\n")

if __name__ == "__main__":
    # Handling commandline arguments
    parser = ArgumentParser(usage=__doc__)
    parser.add_argument(dest="filename", help="The name of the netcdf file you want to analyze")
    args = parser.parse_args()
    main(args.filename)

