import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import traceback


LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

class Logger:

    def __init__(self):
        self._root_log_level = logging.DEBUG
        self._root_log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
        self._date_format = '%Y-%m-%d %H:%M:%S'
        
        self._log_file_directory = Path(Path(__file__).parent, 'log')
        self._log_file_name = 'svea_data_manager.log'
        self._rotating_file_handler = None

        self._setup_logger()
        
    def __del__(self): 
        if not self._rotating_file_handler:
            return
        self._rotating_file_handler.close()
        logging.getLogger().removeHandler(self._rotating_file_handler)
            

    def _setup_logger(self):
        logging.basicConfig(level=self._root_log_level,
                            format=self._root_log_format,
                            datefmt=self._date_format)
        
    @property
    def log_file_path(self):
        return Path(self._log_file_directory, self._log_file_name)

    def set_root_log_level(self, level):
        """ 
        Setting the level of the root logger. 
        Incorrect level does not raise an error!
        """
        lev = LEVELS.get(level.upper(), None)
        if lev is None:
            print(f'Invalid log level "{level}"')
            return
        logging.getLogger().setLevel(lev)
        self._root_log_level = lev
        
    def add_file_handler(self, directory=None):
        """
        Adding a TimedRotatingFileHandler. Option do give directory of the log files This will override the default location. 
        """
        try:
            if directory: 
                self._log_file_directory = Path(directory)
            self._log_file_directory.mkdir(parents=True, exist_ok=True)
            self._rotating_file_handler = TimedRotatingFileHandler(str(self.log_file_path), when='S', interval=1, backupCount=10)
            self._rotating_file_handler.setFormatter(self._root_log_format)
            logging.getLogger().addHandler(self._rotating_file_handler)
        except:
            print(traceback.format_exc())
            return
        
    
if __name__ == '__main__':

    log = Logger()
    log.set_root_log_level('DEBUG')
    log.add_file_handler()
    
    logger = logging.getLogger()
    logger.info('test')
 