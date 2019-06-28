# Altinity Datasets for ClickHouse

Welcome!  `altinity-datasets` loads test datasets for ClickHouse.  It is 
inspired by Python libraries that [automatically load standard datasets](https://scikit-learn.org/stable/modules/generated/sklearn.datasets.load_iris.html#sklearn.datasets.load_iris) 
for quick testing.  

## Getting Started

Altinity-datasets requires Python 3.5 or greater. The `clickhouse-client` 
executable must be in the path to load data. 

Before starting you must install the altinity-datasets package using
pip3. Following example shows install into a Python virtual environment. 
First command is only required if you don't have clickhouse-client already
installed on the host. 

```
sudo apt install clickhouse-client
sudo pip3 install altinity-datasets
```

Many users will prefer to install within a Python3 virtual environment, 
for example:

```
python3 -m venv my-env
. my-env/bin/activate
pip3 install altinity-datasets
```

You can also install a current version directly from Github:
```
pip3 install git+https://github.com/altinity/altinity-datasets.git
```
To remove altinity-datasets run the following command:
```
pip3 uninstall altinity-datasets
```

## Using datasets

The `ad-cli` command manages datasets.  You can see available commands by
typing `ad-cli -h/--help`. All subcommands also accept -h/--help options.

### Listing repos

Let's start by listing repos, which are locations that contain datasets. 

```
ad-cli repo list
```

This will return a list of repos that have datasets.  For the time being there
is just a built-in repo that is part of the altinity-datasets package. 

### Finding datasets

Next let's see the available datasets.  
```
ad-cli dataset search
```
This gives you a list of datasets with detailed descriptions.  You can 
restrict the search to a single dataset by typing the name, for example
`ad-cli search wine`.  You can also search other repos using the repo 
file system location, e.g., `ad-cli search wine --repo-path=$HOME/myrepo`.

### Loading datasets

Now, let's load a dataset.  Here's a command to load the iris dataset
to a ClickHouse server running on localhost.

```
ad-cli dataset load iris
```

Here is a more complex example.  It loads the iris dataset to the `iris_new`
database on a remote server.  Also, we parallize the upload with 10 threads. 
```
ad-cli load iris --database=iris_new --host=my.remote.host.com --parallel=10
```

The command shown above is typical of the invocation when loading on a 
server that has a large number of cores and fast storage. 

Note that it's common to reload datasets expecially during development.
You can do this using `ad-cli load --clean`.  IMPORTANT:  This drops the
database to get rid of dataset tables.  If you have other tables in the
same database they will be dropped as well.

### Dumping datasets

You can make a dataset from any existing table or tables in ClickHouse 
that reside in a single database.  Here's a simple example that shows 
how to dump the weather dataset to create a new dataset. (The weather
dataset is a built-in that loads by default to the weather database.)

```
ad-cli dataset dump weather
```

There are additional options to control dataset dumps.  For example,
we can rename the dateset, restrict the dump to tables that start with
'central', compress data, and overwrite any existing data in the output
directory.

```
ad-cli dataset dump new_weather -d weather --tables='^central' --compress \
  --overwrite
```

### Extra Connection Options

The dataset load and dump commands by default connect to ClickHouse
running on localhost with default user and empty password. The following
example options connect using encrypted communications to a specific
server with explicit user name and password. The last option suppresses
certificate verification.  

```
ad-cli dataset load iris -H 127.0.0.1 -P 9440 \
-u special -p secret --secure --no-verify 
```

Note: To use --no-verify you must also ensure that clickhouse-client is
configured to accept invalid certificates. Validate by logging in using
clickhouse-client with the --secure option.  Check and correct settings
in /etc/clickhouse-client/config.xml if you have problems.

## Repo and Dataset Format

Repos are directories on the file system.  The exact location of the repo is 
known as the repo path.  Data sets under the repo are child directories that
in turn have subdirectories for DDL commands and data.  The following listing 
shows part of the organization of the built-ins repo. 

```
built-ins/
  iris/
    data/
      iris/
        iris.csv
    ddl/
      iris.sql
    manifest.yaml
  wine/
    data/
      wine/
        wine.csv
    ddl/
      wine.sql
    manifest.yaml
```

To create your own dataset you can dump existing tables using `ad-cli dataset 
dump` or copy the examples in built-ins.  The format is is simple. 

* The manifest.yaml file describes the dataset.  If you put in extra fields 
  they will be ignored. 
* The DDL directory contains SQL scripts to run.  By convention these should
  be named for the objects (i.e., tables) that they create. 
* The data directory contains CSV data.  There is a separate subdirectory 
  for each table to be loaded.  Its name must match the table name exactly.
* CSV files can be uncompressed .csv or gzipped .csv.gz.  No other formats
  are supported and the file types must be correctly specified. 

You can place new repos in any location you please.  To load from your 
own repo run a load command and use the --repo-path option to point to the
repo location.  Here's an example:

```
ad-cli dataset load mydataset --repo-path=$HOME/my-repo
```

## Development

To work on altinity-datasets clone from Github and install.  
```
git clone https://github.com/altinity/altinity-datasets.git
cd altinity-datasets
python3 setup.py develop 
```

After making changes you should run tests.
```
cd tests
python3 -m unittest --verbose
```

The following commands build an installable and push to pypi.org.
PyPI account credentials must be set in TWINE_USERNAME and TWINE_PASSWORD.

```
python3 setup.py sdist
twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
```

Code conventions are enforced using yapf and flake8. Run the
dev-format-code.sh script to check formatting.

Run tests as follows with virtual environment set.  You will need a
ClickHouse server with a null password on the default user.

```
cd tests
python3 -m unittest -v
```

## Errors

### Out-of-date pip3 causes installation failure

If pip3 installs with the message `error: invalid command 'bdist_wheel'` you 
may need to upgrade pip.  Run `pip3 install --upgrade pip` to correct the
problem. 

### Materialized views cannot be dumped

ad-cli will fail with an error if you try to dump a database that has
materialized views. The workaround is to omit them from the dump operation 
using a table regex as shown in the following example: 

```
ad-cli dataset dump nyc_taxi_rides --repo-path=.  --compress --parallel=6 \
--tables='^(tripdata|taxi_zones|central_park_weather_observations)$'
```

### --no-verify option fails on self-signed certs

When using ad-cli --secure together with --no-verify options you need
to also configure clickhouse-client to skip certificate verification.
This only applies when the certificate is self-signed.  You must
change /etc/clickhouse-client/config.xml as follows to skip certificate
validation:

```
<config>
    <openSSL>
        <client> <!-- Used for connection to server's secure tcp port -->
            ...
            <invalidCertificateHandler>
                <name>AcceptCertificateHandler</name>
            </invalidCertificateHandler>
        </client>
    </openSSL>
    ...
</config>

```

## Limitations

The most important are:

* Error handling is spotty. If clickhouse-client is not in the path 
  things may fail mysteriously. 
* Datasets have to be on the local file system.  In the future we will 
  use cloud object storage such as S3.

Please file issues at https://github.com/Altinity/altinity-datasets/issues.
Pull requests to fix problems are welcome. 
