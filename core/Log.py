import logging
import sys


# Redirect print() to logging
class PrintLogger:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
    
    def write(self, message):
        if message.strip() != "":  # Ignore empty messages
            self.logger.log(self.level, message)
    
    def flush(self):
        pass  # Needed for compatibility with Python's file-like objects



class Log:
  def __init__(self,file_name):
    global sys
    import sys
    global os
    import os
    global logging
    import logging
    # Redirect stdout and stderr to the logger
  
    # Configure logging to log to a file
    logging.basicConfig(filename=f'{file_name}.log', level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    sys.stdout = PrintLogger(logging.getLogger(), logging.INFO)
    sys.stderr = PrintLogger(logging.getLogger(), logging.ERROR)
    print("Logger Status Check")
