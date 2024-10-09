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
- Start your pipeline files with the commands:
  	- ``cd Pipeline/`` (If this is not already your current directory).
	- ``pipeline <YOUR FILE NAME>``.
 ## Example Usage:
 - Make sure ``sample.xml`` exists in the ``pipelines/`` directory.
 - ``pipeline sample``.
