# airflow-dags
## Virtual enviroment installation ##

Make sure Python 3 is properly installed.
The steps below will install a new Python 3 virtual environment, to use it as Airflow's environment.

```bash
pip3 install virtualenv
python3 -m virtualenv -p python3 <your_venv_folder>
```

Once Airflow's virtual environment installed, the next steps will be install Apache Airflow in it.

```bash
#linux
. <your_venv_folder>/bin/activate

pip install apache-airflow
pip install 'apache-airflow[postgres]'
pip install cryptography
```

Then, start Airflow DB, Scheduler and Webserver.

```bash
#linux
airflow initdb
airflow scheduler
airflow webserver
```

[Option step]:

When running scheduler and webserver on a terminal, it will lock your session, so you can use some kind of session manager tool like Tmux

* tmux new -s <session_name> -> create a new session
* tmux ls -> list sessions
* tmux a -t <session_name> -> attach session
* ctrl+b d -> deattach session

As default, Airflow will create a default folder within SQLITE DB, config and log files.
* On Linux it's usually on **~/airflow/**
