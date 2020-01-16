import configparser

from jinja2 import Template

from .exceptions import OozieError


def _read_properties(path):
    """Read Java style properties file

    :param path: pathlib path to .properties file
    :return: dictionary containing the properties
    """
    # configparser needs a section header to be able to parse the file
    content = "[dummy-section]\n" + path.read_text()

    # parse file
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read_string(content)

    # create dictionary
    properties = dict(config["dummy-section"])
    return properties


def read_user_properties(principal, conf):
    """Get the project properties for the current user.

    Properties are merged from:

        * default.properties (which must define at least a ``project`` property).
        * <user>.properties (if exists).

    The properties coming from the user have greater precedence.

    In addition, the following properties are added unless already defined:

        * project: project name
        * user: owner of the current keytab
        * db: string of the form ``<lowercase user>_<project>``
    """
    # get base properties derived from the project configuration
    project = conf["project"]
    project_entrypoint = conf["project_entrypoint"]
    user = principal
    db_template = Template(conf["default_db_name"])
    db = db_template.render(
        project=project.lower().replace('-', '_'),
        user=user.lower().replace('-', '_')
    )
    properties = {
        "project": project,
        "project_entrypoint": project_entrypoint,
        "user": user,
        "db": db,
    }

    # get default properties
    try:
        default_properties = _read_properties(conf["properties_dir"] / "default.properties")
    except FileNotFoundError:
        raise OozieError("Can't find 'default.properties' file")

    # get user properties
    try:
        user_properties = _read_properties(conf["properties_dir"] / f"{user}.properties")
    except FileNotFoundError:
        user_properties = {}

    # merge contents
    properties.update(default_properties)
    properties.update(user_properties)

    return properties
