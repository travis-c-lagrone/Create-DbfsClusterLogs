import pdb

from pathlib import Path

from clusterlogs import find_first_entry_timestamp, match_to_datetime

path = r'C:\Users\trlagron\Downloads\log4j-active.log'

pdb.run("find_first_entry_timestamp",
        locals={
            "find_first_entry_timestamp": find_first_entry_timestamp,
            "path": path})
