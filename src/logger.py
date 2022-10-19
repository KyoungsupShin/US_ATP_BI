import logging
import logging.handlers
import inspect
import os

class Report_Log:
    log = None
    dir_name = ''
    file_name = ''
    
    def __init__(self, file_name, propagate = True):
        self.file_name = file_name
        Report_Log.log = None
        Report_Log.dir_name = os.path.dirname(os.path.abspath(__file__)) + '/log/'   
        os.makedirs(Report_Log.dir_name, exist_ok = True)
        self.propagate = propagate
        Report_Log.Create_Log(self)
              
    def Create_Log(self) :
        file_max_byte = 1024 * 1024 * 10
        log_name = Report_Log.dir_name + self.file_name + '.log'
        # log_name = os.getcwd() + '/' + self.file_name + '.log'
        print(log_name)

        Report_Log.log = logging.getLogger('Report_Log')
        Report_Log.log.setLevel(logging.DEBUG)

        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s]  %(message)s')

        fileHandler = logging.handlers.RotatingFileHandler(log_name, maxBytes=file_max_byte, backupCount=10,  encoding='utf-8')
        streamHandler = logging.StreamHandler()

        fileHandler.setFormatter(formatter)
        streamHandler.setFormatter(formatter)
        if self.propagate == True:
            Report_Log.log.addHandler(fileHandler)
            Report_Log.log.addHandler(streamHandler)
        
    def Log_Write(self, msg, level='debug') :
        if level == 'debug' :
            Report_Log.log.debug(msg)
        elif level == 'info' :
            Report_Log.log.info(msg)
        elif level == 'warning' :
            Report_Log.log.warning(msg)
        elif level == 'error' :
            Report_Log.log.error(msg)
        elif level == 'critical' :
            Report_Log.log.critical(msg)
        else :
            Report_Log.log.debug(msg)
