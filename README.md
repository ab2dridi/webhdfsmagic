![Version](https://img.shields.io/badge/version-0.4.0-blue.svg)

# webhdfsmagic

**webhdfsmagic** is a Python package that provides IPython magic commands to interact with HDFS via WebHDFS/Knox.  
It supports common HDFS operations such as listing, uploading, downloading, and managing file permissions and ownership—all directly from your Jupyter notebooks.

## Features

- **List Files:**  
  `%hdfs ls [path]` lists files in the specified HDFS directory.

- **Create Directory:**  
  `%hdfs mkdir <path>` creates a new directory on HDFS.

- **Delete Files/Directories:**  
  `%hdfs rm <path or pattern> [-r]` deletes files or directories on HDFS. Wildcards are supported (e.g. `%hdfs rm /user/files*`).

- **Upload Files:**  
  `%hdfs put <local_file_or_pattern> <hdfs_destination>` uploads one or more local files to HDFS.  
  For large files, the upload is done using streaming to avoid high memory consumption.

- **Download Files:**  
  `%hdfs get <hdfs_file_or_pattern> <local_destination>` downloads files from HDFS to your local machine.  
  Streaming is used for downloads to properly handle large files.

- **Display File Content:**  
  `%hdfs cat <file> [-n <number_of_lines>]` displays the content of a HDFS file.  
  By default, the first 100 lines are shown. Use `-n -1` to display the full file.

- **Modify Permissions/Ownership:**  
  `%hdfs chmod [-R] <permission> <path>` and `%hdfs chown [-R] <user:group> <path>` allow you to change file permissions and owner/group recursively.

- **Dynamic Configuration:** Use `%hdfs setconfig { ... }` to update configuration directly in the notebook.

## Installation

Clone the repository and install the package in editable mode for development:

```bash
pip install -e .
```

Alternatively, you can build the package using:

```bash
python setup.py sdist bdist_wheel
```

This command creates a source distribution and a wheel in the dist/ directory.

## Usage
Load the extension in your Jupyter notebook:

```bash
%load_ext webhdfsmagic
Then, you can use the available commands. For example:
```

```bash
# List files on HDFS root
%hdfs ls /

# Upload multiple CSV files from the local directory to HDFS
%hdfs put ~/data/*.csv /user/hdfs/data/

# Download a file from HDFS to the current directory
%hdfs get /user/hdfs/data/sample.csv .

# Display the first 50 lines of a HDFS file
%hdfs cat /user/hdfs/data/sample.csv -n 50
```

## Running Tests
The package uses pytest for unit testing. To run the tests, execute:

```
pytest
```
Make sure you run the tests from the project root or have your PYTHONPATH properly set (e.g., export PYTHONPATH=$PWD).

## Configuration File
### Overview
The package relies on configuration files to set connection parameters (Knox URL, WebHDFS API endpoint, authentication credentials, and SSL verification).
It supports two configuration files in a prioritized order:

`~/.webhdfsmagic/config.json` (Highest Priority):
If present, this file is used to load the configuration directly.

`~/.sparkmagic/config.json` (Fallback):
If the above file is absent, the package will attempt to load configuration from Sparkmagic's file.
It then extracts the URL found in `"kernel_python_credentials": { "url": ... }` and splits it by removing the last segment.
For example, if the URL is https://hostname:port/gateway/default/livy_for_spark3 or https://hostname:port/gateway/default/my_livy, the package will keep only the base URL:
https://hostname:port/gateway/default and then append /webhdfs/v1.

### SSL Verification (verify_ssl)
The parameter `verify_ssl` controls SSL certificate verification:

It can be set as a boolean (`true`/`false`).

Alternatively, it can be a string representing the path to a certificate file.

For Sparkmagic-based configuration, `verify_ssl` is set to `false` by default.
If you wish to enable SSL verification with a custom certificate, simply set verify_ssl to the path of your certificate file in your configuration file.

### Example Configuration Files
Example for `~/.webhdfsmagic/config.json`:

```
{
  "knox_url": "https://hostname:port/gateway/default",
  "webhdfs_api": "/webhdfs/v1",
  "username": "your_username",
  "password": "your_password",
  "verify_ssl": "/path/to/your/cacert.pem"
}
```

Example for `~/.sparkmagic/config.json`:

```
{
  "kernel_python_credentials": {
    "username": "user",
    "password": "password",
    "url": "https://hostname:port/gateway/default/livy_for_spark3",
    "auth": "Basic_Access"
  },
  ...
}
```

In this case, the package will extract the base URL (`https://hostname:port/gateway/default`) from the Sparkmagic configuration, then set the Knox URL for WebHDFS to `https://hostname:port/gateway/default/webhdfs/v1`, and use the `username` and `password` provided. The SSL verification will default to `false` unless overridden.



