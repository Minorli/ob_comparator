# README

This is a complete Python program package, consisting of three parts:
1.  Usage Instructions (README): How to install, configure, and run.
2.  Configuration File (db.ini): A template for you to fill in.
3.  Rules File (remap_rules.txt): An example.
4.  Python Script (db_comparator.py): The core code of the program.

## 1. Usage Instructions (README)

### Purpose:
This program is used to compare TABLE and VIEW objects between Oracle (source) and OceanBase Oracle-compatible tenant (target).
It will strictly follow your configuration (db.ini) and rules (remap_rules.txt) to generate a "final verification checklist", and then check each item:
*   VIEW (View): Only checks for existence in the target.
*   TABLE (Table): Checks for existence, and whether all column names are exactly consistent with the source.

### Installation Dependencies:
This program maximizes the use of Python's standard library. The only external library that needs to be installed is `oracledb` (for connecting to the source Oracle).
```bash
pip install oracledb
```

### Steps to run the program:
1.  **Create files**: Save the following three files (`db.ini`, `remap_rules.txt`, `db_comparator.py`) in the same directory.
2.  **Configure db.ini**: Fill in your Oracle and OceanBase connection information in detail.
3.  **Configure remap_rules.txt**: Fill in all objects that need to be "remapped".
4.  **Execute the program**:

### Offline Package Usage:
The following describes how to use the offline package. Python 3 needs to be installed on the Linux host. Tested with Python 3.7.

```bash
tar -zxvf pa_comparator_offline_pkg.tar.gz
cd pa_comparator

python3.7 -m venv venv
source venv/bin/activate

# (Ensure (venv) is active)
pip install --upgrade pip

# Tell pip to install from our local wheelhouse
pip install --no-index --find-links=wheelhouse oracledb cryptography

python3 db_comparator_fixup_0.2.py
```
