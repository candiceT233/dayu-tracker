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
        self.vfd_tracker_name = 'hdf5_tracker_vfd'
        self.vol_tracker_name = 'tracker'
        
        self.vfd_env_vars = ['HDF5_DRIVER', 'HDF5_DRIVER_CONFIG', 'HDF5_PLUGIN_PATH', 'STAT_FILE_PATH']
        self.vol_env_vars = ['HDF5_VOL_CONNECTOR', 'HDF5_PLUGIN_PATH']

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
                'required': False,
            },
            {
                'name': 'dayu_lib',
                'msg': 'Absolute path to the DaYu library',
                'type': str,
                'default': f"{Package(self.pkg_type).pkg_root}/src/lib",
            },
            {
                'name': 'vfd_tracker',
                'msg': 'Whether to use the VFD tracker',
                'type': bool,
                'default': True,
            },
            {
                'name': 'vol_tracker',
                'msg': 'Whether to use the VOL tracker',
                'type': bool,
                'default': False,
            },
            {
                'name': 'tracker_page_size',
                'msg': 'The page size for the tracker to record with (in bytes)',
                'type': int,
                'default': 65536, #64KB
            },
            {
                'name': 'stat_file_path',
                'msg': 'The path to the file to store the tracker statistics',
                'type': str,
                'default': f"{Package(self.pkg_type).pkg_root}/src/stat",
            }
        ]
    
    def _unset_conda_vars(self,env_vars_toset):
        cmd = ['conda', 'env', 'config', 'vars', 'unset',]
        
        for env_var in env_vars_toset:
            cmd.append(f'{env_var}')
        cmd.append('-n')
        cmd.append(self.config['conda_env'])
        
        cmd = ' '.join(cmd)
        Exec(cmd, LocalExecInfo(env=self.mod_env,))
        self.log(f"DDMD _unset_vfd_vars: {cmd}")
    
    def _set_conda_vars(self,env_vars_toset):
        # Get current environment variables
        cmd = [ 'conda', 'env', 'config', 'vars', 'set']
        for env_var in env_vars_toset:
            env_var_val = self.env[env_var]
            if env_var_val:
                cmd.append(f'{env_var}={env_var_val}')
        
        cmd.append('-n')
        cmd.append(self.config['conda_env'])
        cmd = ' '.join(cmd)
        Exec(cmd, LocalExecInfo(env=self.mod_env,))
        self.log(f"DDMD _set_env_vars: {cmd}")
            
    
    def _setup_vfd_tracker(self):
        """
        Set up the VFD tracker environment variables.

        :return: None
        """
        
        # Set current environment variables
        self.env['HDF5_DRIVER'] = self.vfd_tracker_name
        self.env['HDF5_DRIVER_CONFIG'] = f'true {self.config["tracker_page_size"]}'
        
        # if not empty, append the current HDF5_PLUGIN_PATH to the new plugin path
        if self.env['HDF5_PLUGIN_PATH'] != "":        
            self.env['HDF5_PLUGIN_PATH'] = self.config['dayu_lib'] + "/vfd" + ":" + self.env['HDF5_PLUGIN_PATH']
        else:
            self.env['HDF5_PLUGIN_PATH'] = self.config['dayu_lib'] + "/vfd"
        self.env['STAT_FILE_PATH'] = self.config['stat_file_path']
        
        # Set conda environment variables if required
        if self.config['conda_env']:
            self._unset_conda_vars(self.vfd_env_vars)
            self._set_conda_vars(self.vfd_env_vars)
    
    def _setup_vol_tracker(self):
        """
        Set up the volume tracker environment variables.

        :return: None
        """
        self.vol_env_vars = ['HDF5_VOL_CONNECTOR', 'HDF5_PLUGIN_PATH']
        vol_connector_str = (
            f"{self.vol_tracker_name} under_vol=0;" 
            + "under_info={};" 
            + f"path={self.config['stat_file_path']};"
            + "level=2;format="
            )
        self.log(f"HDF5_VOL_CONNECTOR: {vol_connector_str}")
        
        self.env['HDF5_VOL_CONNECTOR'] = vol_connector_str
        
        # if not empty, append the current HDF5_PLUGIN_PATH to the new plugin path
        if self.env['HDF5_PLUGIN_PATH'] != "":        
            self.env['HDF5_PLUGIN_PATH'] = self.config['dayu_lib'] + "/vol" + ":" + self.env['HDF5_PLUGIN_PATH']
        else:
            self.env['HDF5_PLUGIN_PATH'] = self.config['dayu_lib'] + "/vol"
        
        # Set conda environment variables if required
        if self.config['conda_env']:
            self._unset_conda_vars(self.vol_env_vars)
            self._set_conda_vars(self.vol_env_vars)
        
    
    def _configure(self, **kwargs):
        """
        Converts the Jarvis configuration to application-specific configuration.
        E.g., OrangeFS produces an orangefs.xml file.

        :param kwargs: Configuration parameters for this pkg.
        :return: None
        """
        # check if stat_file_path exist, if not make it
        pathlib.Path(self.config['stat_file_path']).mkdir(parents=True, exist_ok=True)

    def modify_env(self):
        """
        Modify the jarvis environment.

        :return: None
        """
        # Cleanup the HDF5_PLUGIN_PATH variables first, this is run before any application
        self.env['HDF5_PLUGIN_PATH'] = ""
        
        if self.config['vfd_tracker']:
            self._setup_vfd_tracker()
        if self.config['vol_tracker']:
            self._setup_vol_tracker()
