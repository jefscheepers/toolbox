"""A simple script to test whether you can connect to iRODS"""

import os
import ssl
from irods.session import iRODSSession

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

try: 
    with iRODSSession(irods_env_file=env_file, **ssl_settings) as session:
        zone = session.zone
        home = f"/{zone}/home"
        coll = session.collections.get(home)

except Exception as e:
    print(e)
    print("Was not able to connect to zone.")
else:
    print(f"Succesfully connected to zone {zone}")