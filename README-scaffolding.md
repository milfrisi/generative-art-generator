
wf-scaffolding
==============

Scaffolding gives you a base structure for your Hadoop workflow project,
including commands for:

* Initializing your database(s)
* Deploying your code
* Deploying your package dependencies


Quickstart
----------

wf-scaffolding is a template for your project. To get it, just clone the
respository with the name of your workflow:
```
    git clone ssh://git@git.trivago.trv/mp-ds/wf-scaffolding.git my-project
```

Enter the Docker environment that gives you a command line with Hive, HDFS and
Kerberos support:
```
    docker-compose run -v </PATH/TO/MY.KEYTAB>:/etc/krb5.keytab cmd froggle 
```

Once inside, execute the commands to deploy the project:
```
    db-init --execute
    deploy-src
    deploy-env
```

Now you have your new project deployed in Hadoop with its database initialized.
Just start changing it to create your workflow!

(Although you may want to check out the following instructions to know what's
going on here :D)


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
  `/app/conf/env/requirements.txt`, zip it and send it to HDFS.

To execute them, enter into the provided Docker container (`hadoop-cli`) passing
your personal Kerberos keytab:

```
    docker-compose run -v </PATH/TO/MY.KEYTAB>:/etc/krb5.keytab cmd froggle 
```

(`froggle` here is just a wrapper over zsh that give you a colored prompt so
you easily see that you're inside the container. Just replace it by `bash` if
you prefer using that one.)

Once your project is configured (see next section), you can execute the above
commands directly in that shell, which provides already set-up Hive and HDFS
connectivity.

How to get the code
-------------------

The easier way to get wf-scaffolding is to create your new repository as a clone of it:
```
    git clone ssh://git@git.trivago.trv/mp-ds/wf-scaffolding.git my-project
```
This will allow you to pull in new changes in the future too.

Configuration
-------------

**Workflow**

wf-scaffolding provides a barebones working workflow that you can use as a base to start adding your own code.

The main files are:

```
app/
├── bin/
│   └── run.py                    # main Spark script (pretty generic, may not need too many changes)
├── build                         # directory where zip files to be sent to HDFS are created
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
    └── lib                       # generic code of your application (business agnostic)
```

The summary of steps to adapt the scaffolding to your workflow are:

1. Add the workflow properties to `default.properties`.

   The only required one is `project` that should be your project name and will
   be used to name the directories in HDFS and the jobs in Oozie. Properties
   are used by Oozie and are passed to the db-init scripts so they can be used
   as variables there during the database bootstrapping process.

2. Add your personal properties to `<USER>.properties`. (Optional)

   If you want to override some properties but just for your user, create a
   file `conf/<USER>.properties` with them, where `<USER>` is your
   Kerberos/Hadoop username. wf-scaffold will use your keytab to extract this
   name and will try to find this file. If it is there, the properties in it
   will take precedence over the ones in `default.properties`.

   You can add your personal properties to the workflow repository and have
   them under version control too.

3. Adapt Oozie files. (Optional)

   `conf/oozie/coordinator.xml` and `conf/oozie/workflow.xml` are pretty
   generic, but you may want to take a look at them. In particular, you may
   want to add more arguments passed to your `bin/run.py` script. Since the
   properties are passed to the workflow, you can pass different arguments to
   `bin/run.py` for your user in case it is needed.

4. Adapt `run.py` to call the entrypoint of your code.

   The last step of the main `bin/run.py` script is to call the entrypoint of
   your code with the parameters passed from `workflow.xml`. Import and call
   the function at the end of the file.

   *Important*: note that the import of your function has to be placed at the end
   of the file too, when the file `src.zip` has already been added to the Spark
   context.

5. Add the requirements of your application. (Optional)

   If your code requires the presence of third party Python packages to run,
   add them to `conf/env/requirements.txt`. They will automatically be added to
   the Conda environment that you can deploy together with your application.

6. Add your code.

   You can add the code of your workflow/application to `src/app` and the
   generic code that is not business specific into `src/lib`.

   It can be a good idea to add different directories inside `src/app` for the
   different business partitions of the workflow, and inside each partition to
   have all the tables, schemas or aggregation functions related to it.

7. Define how to initialize the database.

   Use the variable `db_init_scripts` in `/app/src/app/settings.py` to list all
   the SQL scripts that have to be run to create a brand new database.

   For example::

```
    from app.directories import project_dir
    db_init_scripts = [
        project_dir / "src/app/foo/sql/create_db.sql",
        project_dir / "src/app/foo/sql/table_foobar.sql",
    ]
```

   The scripts can be located anywhere in your project structure and will be
   executed sequentially. That means that, most likely, you want to create
   first your database(s), then your tables and then insert data into them if
   necessary.

8. Finally and importantly: write a README file!

   A template README file is provided at the root of the repository,
   with headers for the sections that should be covered.

Once you have your workflow configured, to create the database and deploy it in
your workspace, execute:
```
    docker-compose run -v </PATH/TO/MY.KEYTAB>:/etc/krb5.keytab cmd froggle 
    /app# db-init --execute
    /app# deploy-src
    /app# deploy-env
```
Once you have a database and your environment in HDFS already, you can iterate
on your code and deploy it with just:
```
    /app# deploy-src
```

**Note:** wf-scaffolding can create a new database and override your files
every time you deploy, but it will not delete anything in HDFS/Hive by itself
for safety reasons. That means that if you want to start from a clean state you
have to drop the database or delete the project folder yourself.

Other commands
--------------

Apart from the above mentioned commands, wf-scaffold includes some additional
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

