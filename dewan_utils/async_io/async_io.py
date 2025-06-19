from concurrent.futures import ThreadPoolExecutor
from typing import Union

import os
import logging
import pandas as pd


class AsyncIO(ThreadPoolExecutor):
    """
    AsyncIO class instantiates a ThreadPoolExecuter to allow for asyncronous saving of different files while
    the main thread continues execution. AsyncIO inherits from ThreadPoolExecuter.
    """

    def __init__(
        self,
        logger: logging.Logger = None,
        logfile: os.PathLike = None,
        *args,
        **kwargs,
    ) -> None:
        """
        Constructs an AsyncIO object

        Parameters
        ----------
        logger (logging.Logger):
            An instance of logging.Logger can be supplied to write log messages from threads. If none is supplied,
             a new logging.Logger instance is created (default is None)

        logfile (os.PathLike):
            Path to a logfile (with extension) can be provided to save log output to disk (default is None)

        *args
            Additional arguments should be passed as keyword arguments
        **kwargs
            Extra arguments to `__init__`: refer to documentation for a
            list of all possible arguments.
        """
        super().__init__()

        self.logger: logging.Logger = Union[None, logging.Logger]
        self.setup_logger(logger, logfile)

    def setup_logger(self, logger: logging.Logger, logfile: os.PathLike):
        """
        Function checks if the user supplied a Logger or logfile path. If no Logger is supplied, a new one is instantiated.
        If a logfile path is provided, its added as a handler to the logger. Once finished, the class's logger field is
        set to the final Logger
        Parameters
        ----------
        logger (logging.Logger):
            An instance of logging.Logger can be supplied to write log messages from threads. If none is supplied,
             a new logging.Logger instance is created (default is None)

        logfile (os.PathLike):
            Path to a logfile (with extension) can be provided to save log output to disk (default is None)

        Returns
        -------
        None
        """

        if logger is None:
            logger = logging.getLogger(__name__)
        if logfile:
            logger.addHandler(logging.FileHandler(logfile))

        logging.basicConfig(level=logging.NOTSET)
        self.logger = logger

    def queue_save_df(self, df_to_save: pd.DataFrame, file_path: os.PathLike) -> None:
        """
        Public function to queue a Pandas Dataframe to be saved to disk
        Parameters
        ----------
        df_to_save (Pandas.DataFrame):
            Pandas dataframe the user wishes to save to disk
        file_path (os.PathLike):
            File path with extension pointing to the save directory

        Returns
        -------
            None
        """
        self.submit(self._save_df, df_to_save, file_path)

    def _save_df(self, df_to_save: pd.DataFrame, file_path: os.PathLike) -> None:
        """
        Private function that saves a Dataframe. This function is submitted to the ThreadPoolExecuter as a job
        Parameters
        ----------
        df_to_save (Pandas.DataFrame):
            Pandas dataframe the user wishes to save to disk
        file_path (os.PathLike):
            File path with extension pointing to the save directory

        Returns
        -------
            None
        """
        try:
            df_to_save.to_excel(file_path)
        except Exception:
            self.logger.error("Unable to save %s", file_path)
        else:
            self.logger.info("Saved %s", file_path)
