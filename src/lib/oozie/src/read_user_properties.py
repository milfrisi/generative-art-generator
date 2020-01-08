from ..exceptions import OozieError
from .read_properties import read_properties


def read_user_properties(directory, user):
    """Get the project properties for the current user.

    Properties are merged from:

        * <directory>/default.properties (which must define at least a ``project`` property).
        * <directory>/<user>.properties (if exists).

    The properties coming from the user have greater precedence.

    In addition, the following properties are added unless already defined:

        * user: principal of the current keytab
        * db: string of the form ``<lowercase user>_<project>``
    """
    # get default properties
    try:
        default_properties = read_properties(directory / "default.properties")
    except FileNotFoundError:
        raise OozieError(f"Can't find 'default.properties' file")

    # get user properties
    try:
        user_properties = read_properties(directory / f"{user}.properties")
    except FileNotFoundError:
        user_properties = {}

    # merge contents
    merged_properties = {}
    merged_properties.update(default_properties)
    merged_properties.update(user_properties)

    # get db name
    if "project" not in merged_properties:
        raise OozieError(f"'project' property not defined.")
    db = f'{ user.lower() }_{ merged_properties["project"] }'

    # get final properties
    properties = {
        "user": user,
        "db": db,
    }
    properties.update(merged_properties)

    return properties
