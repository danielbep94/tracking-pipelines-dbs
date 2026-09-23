# logs/logger.py
import logging

def get_logger(name="ETL_Logger"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
        
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
        # Prevent Databricks root logger from duplicating these messages
        logger.propagate = False 
        
    return logger