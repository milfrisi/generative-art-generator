
from pathlib import Path

import toml
import yaml
from cerberus import Validator

from .exceptions import ConfError

conf_schema = {
    'project': {
        'type': 'string',
        'required': True,
        'nullable': False,
    },
    'project_type': {
        'type': 'string',
        'required': False,
        'allowed': ['pyspark', 'hive'],
        'default': 'pyspark',
    },
    'properties_dir': {
        'type': 'string',
        'required': False,
        'default': 'conf/',
    },
    'source_dir': {
        'type': 'string',
        'required': False,
        'default': 'src/',
    },
    'oozie_dir': {
        'type': 'string',
        'required': False,
        'default': 'conf/oozie/',
    },
    'requirements_file': {
        'type': 'string',
        'required': False,
        'default': 'conf/env/requirements.txt',
    },
    'project_entrypoint': {
        'type': 'string',
        'required': False,
        'default': '',
    },
    'main_spark_script': {
        'type': 'string',
        'required': False,
        'default': '/app/bin/run.py',
    },
    'default_db_name': {
        'type': 'string',
        'required': False,
        'default': "{{ user }}_{{ project }}",
    },
    'db_init_scripts': {
        'type': 'list',
        'required': False,
        'schema': {
            'type': 'string',
        },
    },
}

def get_project_conf(conf_filename):

    # read configuration file
    try:
        conf_data = toml.load(conf_filename)
    except FileNotFoundError as err:
        raise ConfError(f"Can't find the TOML configuration file: {conf_filename}")

    # extract scaffolding section
    try:
        scaffolding_conf = conf_data['tool']['scaffolding']
    except KeyError as err:
        raise ConfError(f"Configuration file '{conf_filename}' doesn't contain a [tool.scaffolding] section.")

    # validate configuration
    validator = Validator(conf_schema)
    if not validator.validate(scaffolding_conf):
        raise ConfError(f"\n{yaml.dump(validator.errors)}")

    # setup paths
    conf = validator.document.copy()
    conf['project_dir'] = Path(conf_filename).absolute().parent
    conf['properties_dir'] = Path(conf['properties_dir']).absolute()
    conf['requirements_file'] = Path(conf['requirements_file']).absolute()
    conf['main_spark_script'] = Path(conf['main_spark_script']).absolute()
    conf['source_dir'] = Path(conf['source_dir']).absolute()
    conf['oozie_dir'] = Path(conf['oozie_dir']).absolute()
    conf['db_init_scripts'] = [conf['project_dir'] / script for script in conf['db_init_scripts']]

    return conf
