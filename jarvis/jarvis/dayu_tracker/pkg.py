"""
This module provides classes and methods to inject the DayuTracker interceptor.
DayuTracker is ....
"""
from jarvis_cd.basic.pkg import Interceptor
from jarvis_util import *

from scspkg.pkg import Package

class DayuTracker(Interceptor):
    """
    This class provides methods to inject the DayuTracker interceptor.
    """
    def _init(self):
        """
        Initialize paths
        """
        pass

    def _configure_menu(self):
        """
        Create a CLI menu for the configurator method.
        For thorough documentation of these parameters, view:
        https://github.com/scs-lab/jarvis-util/wiki/3.-Argument-Parsing

        :return: List(dict)
        """
        return [
            {
                'name': 'conda_env',
                'msg': 'Name of the conda environment to update parameters',
                'type': str,
                'default': None,
            },
            {
                'name': 'tracker_path',
                'msg': 'Absolute path to the Pyflextrkr source code',
                'type': str,
                'default': f"{Package(self.pkg_type).pkg_root}/src/vol-tracker",
            },
        ]

    def _configure(self, **kwargs):
        """
        Converts the Jarvis configuration to application-specific configuration.
        E.g., OrangeFS produces an orangefs.xml file.

        :param kwargs: Configuration parameters for this pkg.
        :return: None
        """
        pass

    def modify_env(self):
        """
        Modify the jarvis environment.

        :return: None
        """
        pass
