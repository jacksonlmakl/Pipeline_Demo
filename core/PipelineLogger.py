import sys
import os
import logging
import datetime
# Define the PrintLogger class for capturing stdout and stderr
class PrintLogger:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message.strip():  # Ignore empty messages
            self.logger.log(self.level, message)

    def flush(self):
        pass  # For file-like object compatibility


class PipelineLogger:
    def __init__(self):
        file_name=datetime.datetime.now().__str__().replace("-","_").replace(" ","__").replace(":","_").split(".")[0]
        print(f"Saving log to {file_name}")
        # Configure logging to log to a file
        logging.basicConfig(
            filename=f'log__{file_name}.log', 
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

        # Redirect stdout and stderr to the logger
        sys.stdout = PrintLogger(logging.getLogger(), logging.INFO)
        sys.stderr = PrintLogger(logging.getLogger(), logging.ERROR)

        # Test the logger by printing a message
        print("Logger Status Check")
