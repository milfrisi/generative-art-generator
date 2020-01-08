import re
import subprocess

from .exceptions import KerberosError


def get_kerberos_principal():
    """Get the default kerberos user for the active ticket."""
    # use klist to get info about current ticket
    try:
        output = subprocess.check_output("klist")
    except subprocess.CalledProcessError:
        raise KerberosError("Can't execute 'klist'")

    # extract principal
    principal_pattern = rb"Default principal: ([^@]+)@HADOOP\.TRIVAGO\.COM"
    output_match = re.search(principal_pattern, output)
    if not output_match:
        raise KerberosError(f"Error: can't extract principal from ticket:\n{ output_match }\n")
    principal = output_match.group(1).decode()

    return principal
