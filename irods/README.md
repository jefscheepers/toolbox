# iRODS scripts


## sync_directory.py

Upload a local directory to a destination in iRODS.  
If files already exist, the script compares the local file and the version in iRODS.
When the size/checkum differs, te version in iRODS gets overwritten.

### Usage 
```
# general usage
python sync_directory --verification <verification method> --post-check <source> <destination>

# example
python sync_directory --verification checksum --post-check /home/research/testdata /zone/home/research
```

### Arguments

* --verification: decides how to compare local files an data objects if the data already exists in iRODS. Possible choices are 'size' and 'checksum'.    
* --post-check: when you use this flag, after each upload the script will checksum both the local and uploaded file, to verify the transfer was successful.  
* source: path of a local directory you want to upload. Please provide the full path.   
* destination: the directory you want to upload your data to in iRODS. For example, if you have /home/testdata as source and /zone/home/research as destination, the data will end up in /zone/home/research/testdata.  

### Output

* At of the transfer, a message is printed stating:
    * How many files were skipped (because they were already in iRODS and up to date)
    * How many files were successfully uploaded
    * How many files failed to upload
    * How many bytes the succesfully uploaded files were, in total.   
* A logfile is created.  
    The name starts with sync_log, followed by the time the transfer was started(`sync_log<year><month><day><hour><minute><second>.json`).   
    It contains:
    * A list of skipped data objects
    * A list of successfully uploaded data objects
    * A list of files that failed to upload
    * How many bytes the succesfully uploaded files were, in total.  
