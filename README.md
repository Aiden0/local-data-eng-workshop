https://airflow.apache.org/docs/apache-airflow/stable/start.html
https://github.com/dbt-labs/jaffle_shop_duckdb/tree/duckdb

# Testing dbt project: `jaffle_shop`

`jaffle_shop` is a fictional ecommerce store. This dbt project transforms raw data from an app database into a customers and orders model ready for analytics.

<details>
<summary>

## What is this repo?

</summary>

What this repo _is_:

- A self-contained playground dbt project, useful for testing out scripts, and communicating some of the core dbt concepts.

What this repo _is not_:

- A tutorial — check out the [Getting Started Tutorial](https://docs.getdbt.com/tutorial/setting-up) for that. Notably, this repo contains some anti-patterns to make it self-contained, namely the use of seeds instead of sources.
- A demonstration of best practices — check out the [dbt Learn Demo](https://github.com/dbt-labs/dbt-learn-demo) repo instead. We want to keep this project as simple as possible. As such, we chose not to implement:
  - our standard file naming patterns (which make more sense on larger projects, rather than this five-model project)
  - a pull request flow
  - CI/CD integrations
- A demonstration of using dbt for a high-complex project, or a demo of advanced features (e.g. macros, packages, hooks, operations) — we're just trying to keep things simple here!

</details>

<details>
<summary>

## What's in this repo?

</summary>

This repo contains [seeds](https://docs.getdbt.com/docs/building-a-dbt-project/seeds) that includes some (fake) raw data from a fictional app along with some basic dbt [models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models), tests, and docs for this data.

The raw data consists of customers, orders, and payments, with the following entity-relationship diagram:

![Jaffle Shop ERD](/etc/jaffle_shop_erd.png)

</details>

## Why should I care about this repo?

If you're just starting your cloud data warehouse journey and are hungry to get started with dbt before your organization officially gets a data warehouse, you should check out this repo.

If you want to run 28 SQL operations with dbt in less than `1 second`, for free, and all on your local machine, you should check out this repo.
![dbt_performance](images/dbt_performance.png)

If you want an adrenaline rush from a process that used to take dbt newcomers `1 hour` and is now less than `1 minute`, you should check out this repo.

![dbt_full_deploy_commands](images/dbt_full_deploy_commands.png)

[Verified GitHub Action on dbt Performance](https://github.com/dbt-labs/jaffle_shop_duckdb/runs/7141529753?check_suite_focus=true#step:4:306)

## Running this project

Prerequisities: Python >= 3.5

### Step-by-step explanation

To get up and running with this project:

1. Clone this repository.

```shell
git clone https://github.com/dbt-labs/jaffle_shop_duckdb.git

```

1. Change into the `jaffle_shop_duck` directory from the command line:

   ```shell
   cd jaffle_shop_duckdb
   ```

1. Install dbt and DuckDB in a virtual environment.

   Expand your shell below:

   <details open>
   <summary>POSIX bash/zsh</summary>

   ```shell
   python3 -m venv venv
   source venv/bin/activate
   python3 -m pip install --upgrade pip
   python3 -m pip install -r requirements.txt
   source venv/bin/activate
   ```

   </details>

   <details>
   <summary>POSIX fish</summary>

   ```shell
   python3 -m venv venv
   source venv/bin/activate.fish
   python3 -m pip install --upgrade pip
   python3 -m pip install -r requirements.txt
   source venv/bin/activate.fish
   ```

   </details>

   <details>
   <summary>POSIX csh/tcsh</summary>

   ```shell
   python3 -m venv venv
   source venv/bin/activate.csh
   python3 -m pip install --upgrade pip
   python3 -m pip install -r requirements.txt
   source venv/bin/activate.csh
   ```

   </details>

   <details>
   <summary>POSIX PowerShell Core</summary>

   ```shell
   python3 -m venv venv
   venv/bin/Activate.ps1
   python3 -m pip install --upgrade pip
   python3 -m pip install -r requirements.txt
   venv/bin/Activate.ps1
   ```

   </details>

   <details>
   <summary>Windows cmd.exe</summary>

   ```shell
   python -m venv venv
   venv\Scripts\activate.bat
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt
   venv\Scripts\activate.bat
   ```

   </details>

   <details>
   <summary>Windows PowerShell</summary>

   ```shell
   python -m venv venv
   venv\Scripts\Activate.ps1
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt
   venv\Scripts\Activate.ps1
   ```

   </details>

   _Why a 2nd activation of the virtual environment?_
   <details>
   <summary>This may not be necessary for many users, but might be for some. Read on for a first-person report from @dbeatty10.</summary>

   I use `zsh` as my shell on my MacBook Pro, and I use `pyenv` to manage my Python environments. I already had an alpha version of dbt Core 1.2 installed (and yet another via [pipx](https://pypa.github.io/pipx/installation/)):

   ```shell
   $ which dbt
   /Users/dbeatty/.pyenv/shims/dbt
   ```

   ```shell
   $ dbt --version
   Core:
     - installed: 1.2.0-a1
     - latest:    1.1.1    - Ahead of latest version!

   Plugins:
     - bigquery:  1.2.0a1 - Ahead of latest version!
     - snowflake: 1.2.0a1 - Ahead of latest version!
     - redshift:  1.2.0a1 - Ahead of latest version!
     - postgres:  1.2.0a1 - Ahead of latest version!
   ```

   Then I ran all the steps to create a virtual environment and install the requirements of our DuckDB-based Jaffle Shop repo:

   ```shell
   $ python3 -m venv venv
   $ source venv/bin/activate
   (venv) $ python3 -m pip install --upgrade pip
   (venv) $ python3 -m pip install -r requirements.txt
   ```

   Let's examine where `dbt` is installed and which version it is reporting:

   ```shell
   (venv) $ which dbt
   /Users/dbeatty/projects/jaffle_duck/venv/bin/dbt
   ```

   ```shell
   (venv) $ dbt --version
   Core:
     - installed: 1.2.0-a1
     - latest:    1.1.1    - Ahead of latest version!

   Plugins:
     - bigquery:  1.2.0a1 - Ahead of latest version!
     - snowflake: 1.2.0a1 - Ahead of latest version!
     - redshift:  1.2.0a1 - Ahead of latest version!
     - postgres:  1.2.0a1 - Ahead of latest version!
   ```

   ❌ That isn't what we expected -- something isn't right. 😢

   So let's reactivate the virtual environment and try again...

   ```shell
   (venv) $ source venv/bin/activate
   ```

   ```shell
   (venv) $ dbt --version
   Core:
     - installed: 1.1.1
     - latest:    1.1.1 - Up to date!

   Plugins:
     - postgres: 1.1.1 - Up to date!
     - duckdb:   1.1.3 - Up to date!
   ```

   ✅ This is what we want -- the 2nd reactivation worked. 😎
   </details>

1. get the absolute path of this repo, going to use it a lot:

```
echo $(pwd) # /path/to/this/repo/local-data-eng-workshop
```

1. Install Airflow

```shell
# Change to path of the repo with airflow at the end i.e /path/to/this/repo/local-data-eng-workshop/airflow
export AIRFLOW_HOME=$(pwd)/airflow
AIRFLOW_VERSION=2.10.3
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

1. Ensure your [profile](https://docs.getdbt.com/reference/profiles.yml) is setup correctly from the command line:

   ```shell
   dbt --version
   dbt debug
   ```

1. run airflow

```
airflow standalone
```

Then go to the url localhost:8080. It will ask for username and password. The username should be admin, the password is randomly generated and can be found when searching `password` in the logs for command used above.

1. Close airflow, there should be a new folder called `airflow`. Now open the file `airflow/airflow.cfg` and change variable `dags_folder` to folder dags in top directory i.e:

```
dags_folder = /path/to/this/repo/local-data-eng-workshop/dags
```

then go into file `/path/to/this/repo/local-data-eng-workshop/dags/upload_files.py`
and replace the variable `DUCKDB_CONN_ID` with repo path plus the name `jaffle_shop.duckdb` i.e

```python
DUCKDB_CONN_ID = "/path/to/this/repo/local-data-eng-workshop/jaffle_shop.duckdb"
```

Then restart airflow

```
airflow standalone
```

and search for the dag gcs_to_duck_db and run it and monitor it on localhost:8080

1. Now we are going to run out dbt models, and test the output of the models using the [dbt build](https://docs.getdbt.com/reference/commands/build) command:

   ```shell
   dbt build
   ```

1. Query the data:

   Launch a DuckDB command-line interface (CLI):

   ```shell
   duckcli jaffle_shop.duckdb
   ```

   Run a query at the prompt and exit:

   ```
   select * from customers where customer_id = 42;
   exit;
   ```

   Alternatively, use a single-liner to perform the query:

   ```shell
   duckcli jaffle_shop.duckdb -e "select * from customers where customer_id = 42"
   ```

   or:

   ```shell
   echo 'select * from customers where customer_id = 42' | duckcli jaffle_shop.duckdb
   ```

1. Generate and view the documentation for the project:

   ```shell
   dbt docs generate
   dbt docs serve
   ```

1. Now to visualize the data, go to vsc code extesntions and search for evidence

## Browsing the data

Some options:

- [duckcli](https://pypi.org/project/duckcli/)
- [DuckDB CLI](https://duckdb.org/docs/installation/?environment=cli)
- [How to set up DBeaver SQL IDE for DuckDB](https://duckdb.org/docs/guides/sql_editors/dbeaver)

### Troubleshooting

You may get an error like this, in which case you will need to disconnect from any sessions that are locking the database:

```
IO Error: Could not set lock on file "jaffle_shop.duckdb": Resource temporarily unavailable
```

This is a known issue in DuckDB. If you are using DBeaver, this means shutting down DBeaver (merely disconnecting didn't work for me).

Very worst-case, deleting the database file will get you back in action (BUT you will lose all your data).
