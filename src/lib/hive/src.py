import os
import subprocess
from .exceptions import HiveError


def execute_script(text, beeline_cmd="beeline"):
    """Execute a SQL script in Hive using beeline."""
    # create script file
    tmp_filename = f"/tmp/db-init-{ os.getpid() }.sql"
    with open(tmp_filename, "w") as script:
        script.write(text)

    # execute it
    result = subprocess.run([beeline_cmd, "-f", tmp_filename], capture_output=True, text=True)
    if result.returncode != 0:
        raise HiveError(f"Error in hive when executing: '{ text }'\n{ result.stderr }")
