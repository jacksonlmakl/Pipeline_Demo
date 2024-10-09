# Pipeline

## Requirements:
- Python 3.10.
- Unix-based operating system (e.g., Linux, macOS).
- Root access.
## Setup:
- ``git clone https://github.com/jacksonlmakl/Pipeline.git``
- ``cd Pipeline``.
- ``bin/setup``.


## Usage:
- Add Python requirements for your pipeline files in ``requirements.txt`` (Do not remove existing packages).
- Create new pipelines by adding .xml files to ``pipelines/`` (See ``sample.xml`` for formatting).
- Pipeline files support Jinja templating. You can store your variables in the ``variables.json`` configuration file.
- Start your pipeline files with the commands:
  	- ``cd Pipeline/`` (If this is not already your current directory).
	- ``pipeline <YOUR FILE NAME>``.
- Kill processes with the command:
	- ``bin/stop <YOUR FILE NAME>``.
 ## Example Usage:
 - Make sure ``kanto.xml`` & ``johto.xml`` exist in the ``pipelines/`` directory.
 - ``pipeline kanto`` (To start a scheduled pipeline).
 - The Kanto Pipeline will kick off. This runs an ETL Pipeline building tables on Pokemon from the Kanto region. It also kicks off ``johto.xml`` which runs a ETL Pipeline for tables on Pokemon in the Johto region.
 - ``bin/stop kanto`` (To stop a running/scheduled pipeline).
