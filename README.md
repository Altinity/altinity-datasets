# Altinity Datasets for ClickHouse

Welcome!  `altinity-datasets` loads test datasets for ClickHouse.  It is 
inspired by Python libraries that [automatically load standard datasets](https///scikit-learn.org/stable/modules/generated/sklearn.datasets.load_iris.html#sklearn.datasets.load_iris) 
for quick testing.  

## Getting Started

Altinity-datasets requires Python 3.5 or greater. The `clickhouse-client` 
executable must be in the path to load data. 

Before starting you must install the altinity-datasets package using
pip3. Here are two quick options.

Install current version directly from Github/
```
pip3 install --user git+https///github.com/altinity/altinity-datasets.git
```

Install local source/
```
git clone https///github.com/altinity/altinity-datasets.git
cd altinity-datasets
python3 setup.py install --user
```

To remove altinity-datasets run the following command/
```
pip3 uninstall altinity-datasets
```

## Installing datasets.

The `ad-cli` command installs datasets.  Here is a short tutorial.  You can 
see available commands by typing `ad-cli --help`. 

Let's start by listing repos, which are locations that contain datasets. 

```
ad-cli repos
```
This will return a list of repos that have datasets.  For the time being there
is just a built-in repo that is part of the altinity-datasets package. 

Next let's see the available datasets.  
```
ad-cli search
```
This gives you a list of datasets with detailed descriptions.  You can 
restrict the search to a single dataset by typing the name, for example
`ad-cli search wine`.  You can also search other repos using the repo 
file system location, e.g., `ad-cli search wine --repo-path=$HOME/myrepo`.

Finally, let's load a dataset.  This currently only works with ClickHouse
servers that use the default user and unencrypted communications.  (See 
limitations below.) Here's a command to load the iris dataset to a 
ClickHouse server running on localhost. 

```
ad-cli load iris
```

Here is a more complex example.  It loads the airplane dataset to the `air`
database on a remote server.  Also, we parallize the upload with 10 threads. 
```
ad-cli load airline --database=air --host=my.remote.host.com --parallel=10
```

The command shown above is typical of the invocation when loading on a 
server that has a large number of cores and fast storage. 

Note that it's common to reload datasets expecially during development.
You can do this using `ad-cli load --clean`.  IMPORTANT:  This drops the
database to get rid of dataset tables.  If you have other tables in the
same database they will be dropped as well.

## Repo and Dataset Format

Repos are directories on the file system.  The exact location of the repo is 
known as the repo path.  Data sets under the repo are child directories that
in turn have subdirectors for DDL commands and data.  The following listing 
shows the organization of the built-ins repo. 

```
built-ins/
  airline/
    data/
      airports/
        Airports.csv.gz
      ontime/
        On_Time_On_Time_Performance_2018_11.csv.gz
    ddl/
      airports.sql
      ontime.sql
    manifest.yaml
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

To create your own dataset copy the examples in built-ins.  The format is 
is simple.  Here are a couple of notes to get you started. 

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
ad-cli load mydataset --repo-path=$HOME/my-repo
```

## Limitations

Really too many to mention but the most important are:

* Database connection parameters are not supported yet.
* There is no automatic way to populate large dataset like airline/ontime. 
  You can add the extra .zip files yourself. 
