import configparser


def read_properties(path):
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
