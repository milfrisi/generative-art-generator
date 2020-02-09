
wf-scaffolding
==============

The `trv-scaffolding` tool is a Python package that allows you to easily
initialize, deploy and run your workflows in your own personal space.

This respository is a template workflow that you can use to create your own or
to see how you can modify an existing one so you can use the scaffolding
commands.

`trv-scaffolding` supports both Hive and Spark workflows. The commands that
you'll have available are:

* `db-init`: initialize the database in a user space
* `deploy-src`: package and deploy your code
* `deploy-env`: package and deploy your workflow dependencies
* `submit`: run your workflow

This means that you can execute these actions from your local machine as you
modify the code of your workflow allowing you to iterate on your solution
faster.

Dependencies
------------

The only dependency to use these commands is to have Docker installed in your
machine.

All commands have to be run inside a Docker container whose image is provided
and already configured to deal with Kerberos, trivago's Hadoop configuration and
includes Hive and HDFS tools. So basically you get all the configuration and
matching versions once you're inside the command line of that container.

Quickstart
----------

wf-scaffolding is a dummy workflow that you can use to bootstrap your
project.

The first step is to clone and rename it with the name of your project:
```
git clone http://git.trivago.trv/mp-ds/wf-scaffolding.git my-project
```

Then you can inmediately have access to the Docker environment that gives you
Hive, HDFS and Kerberos support. Notice that you have to pass the location of
your Kerberos keytab in your local computer in order for the container to be
able to connect to Hadoop correctly:
```
docker-compose run -v </PATH/TO/MY.KEYTAB>:/etc/krb5.keytab cmd bash
```

Don't do it yet since you may want to configure the project first (see the
Configuration section below), but executing commands will deploy your project
into Hadoop:
```
db-init --execute
deploy-src
deploy-env
```

Now you have your new project in your HDFS workspace with its database
initialized. Just start changing it to create your workflow!

You may want to take a look at the information below first thoug :)

Project structure once deployed
-------------------------------

After deploying your project will look like this:

```
/user/<USER>/db/<PROJECT>.db    # Hive database
/user/<USER>/<PROJECT>/build/   # workflow files (source + Oozie configuration)
/user/<USER>/<PROJECT>/env.zip  # Conda environment containing the wf dependencies
```

And your Hive database will be named (if the default name is not changed):
```
<USER>_<PROJECT>
```

How to run the commands
-----------------------

wf-scaffolding provides three main commands:

* `db-init [--execute]`: initialize database (if `--execute` is not passed, you
  get a dump of all SQL statements that would be sent)

* `deploy-src`: make a zip file with the contents of `/app/src/`, compute the
  workflow configuration for this user and copy everything to HDFS.

* `deploy-env`: create a Conda environment with the packages specified in
  `/app/conf/env/requirements.txt`, zip it and send it to HDFS. You only need to
  execute this the first time you deploy your workflow and every time you change
  the requirements, but not when you change your code. Also: this is only
  required for PySpark workflows.

* `submit <YMD>`: submit your workflow for execution. You'll get back an URL in
  the terminal that you can use to track the progress of your job in your
  browser.

Once your project is configured (see next section), you can execute the above
commands directly in the Docker shell.

Configuration
-------------

**Workflow**

wf-scaffolding provides a barebones working workflow that you can use as a base to start adding your own code.
The Python files are not needed for Hive workflows:

```
app/
├── bin/
│   └── run.py                    # main Spark script
├── conf
│   ├── default.properties        # job.properties is created by merging `default.properties` and
│   ├── <USER>.properties         # `<USER>.properties` (if it exists). `<USER>` is the keytab user.
│   ├── env
│   │   └── requirements.txt      # Python dependencies of your application (always provide exact versions)
│   └── oozie
│       ├── coordinator.xml       # Oozie coordinator (pretty generic)
│       └── workflow.xml          # Oozie workflow (pretty generic)
└── src
    ├── app                       # your application source code
    │   ├── <component-1>         # your application components
    │   ├── <component-2>
    │   ├── directories.py        # key directories of your application defined as pathlib objects
    │   └── settings.py           # settings of your application
    └── lib                       # generic code of your application
```

The summary of steps to adapt the scaffolding to your workflow are:

1. Configure your workflow using `pyproject.toml`:

```
[tool.scaffolding]
project = "scaffolding"
project_type = "pyspark"
db_init_scripts = [
    "src/app/component/sql/create_db.sql",
    "src/app/component/sql/table_ticker.sql",
]
```
   - Set the name of your project.
   - Set the project type (either `pyspark` or `hive`)
   - List the scripts that will be executed to initialize the database. They can
     be anywhere in the folder structure of the repository and will be executed
     sequentially. For example, you can first create the database, then some
     tables and then insert some initial data if needed.

   IMPORTANT! use only alphanumeric characters, `_` and `-` for the name of
   your project. Since it will be used to name the folder in your workspace and
   the Hive database, it can create problems if you add any other type of
   character. In the case of the database name, dashes (`-`) will be
   automatically converted into underscores (`_`) and uppercase letters into
   lowercase ones.

2. Add the workflow properties to `conf/default.properties`.

   Properties are used by Oozie and are passed to the db-init scripts so they
   can be used as variables there during the database bootstrapping process.

3. Add your personal properties to `<USER>.properties`. (Optional)

   If you want to override some properties but just for your user, create a
   file `conf/<USER>.properties` with them, where `<USER>` is your
   Kerberos/Hadoop username. Scaffolding will use your keytab to extract this
   name and will try to find this file. If it is there, the properties in it
   will take precedence over the ones in `default.properties`.

   You can add your personal properties to the workflow repository and have
   them under version control too.

4. Adapt Oozie files. (Optional)

   `conf/oozie/coordinator.xml` and `conf/oozie/workflow.xml` are pretty
   generic, but you may want to take a look at them. In particular, you may
   want to add more arguments passed to your `bin/run.py` script. Since the
   properties are passed to the workflow, you can pass different arguments to
   `bin/run.py` for your user in case it is needed.

5. Adapt `run.py` to call the entrypoint of your code.

   The last step of the main `bin/run.py` script is to call the entrypoint of
   your code with the parameters passed from `workflow.xml`. Import and call
   the function at the end of the file.

   *Important*: note that the import of your function has to be placed at the end
   of the file too, when the file `src.zip` has already been added to the Spark
   context.

6. Add the requirements of your application. (Optional)

   If your code requires the presence of third party Python packages to run,
   add them to `conf/env/requirements.txt`. They will automatically be added to
   the Conda environment that you can deploy together with your application.

7. Add your code.

   You can add the code of your workflow/application to `src/app` and the
   generic code that is not business specific into `src/lib`.

   It can be a good idea to add different directories inside `src/app` for the
   different business partitions of the workflow, and inside each partition to
   have all the tables, schemas or aggregation functions related to it.

8. Finally and importantly: write a README file!

   A template README-project.md file is provided at the root of the repository,
   with headers for the sections that should be covered.

Once you have your workflow configured, to create the database and deploy it in
your workspace, execute:
```
docker-compose run -v </PATH/TO/MY.KEYTAB>:/etc/krb5.keytab cmd bash
/app# db-init --execute
/app# deploy-src
/app# deploy-env
```
After that, most of the time you'll need only to change your code and then do:
```
/app# deploy-src
```

Reset project
-------------

**Important**: For now, Scaffolding won't delete the database for you if you
want to recreate it. For safety reasons you have to do that manually. So if you
want to start from a blank state, you can do something like:

You can get a Hive command line by executing `start-beehive`. Then you can
delete the database with:
```
DROP DATABASE <database_name> cascade;
```

Then you can delete the database files (if necessary) with:
```
hdfs dfs -rm -r /user/<USER>/db/<PROJECT>.db
```

Other commands
--------------

Apart from the above mentioned commands, scaffolding includes some additional
actions that you can run from inside the Docker container:

* `validate`: check the syntax of your `coordinator.xml` and `workflow.xml` files.
* `tests`: run your python tests in the repository
* `black /app/src`: run black code formatter on the project

Also, outside the Docker container, you can run:
```
make init
```
To install a `black` check as a pre-commit hook. That means that if you try to
commit code that doesn't conform to Black rules, the commit will be aborted.
You can fix your code by running `black` itself and adding and committing the
reformatted files again.

