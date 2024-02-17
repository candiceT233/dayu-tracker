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
        
        self.vfd_env_vars = ['HDF5_DRIVER', 'HDF5_DRIVER_CONFIG', 'HDF5_PLUGIN_PATH'] # 
        self.vol_env_vars = ['HDF5_PLUGIN_PATH', 'HDF5_VOL_CONNECTOR'] #
        
        self.dayu_task_vars = ['PATH_FOR_TASK_FILES', "WORKFLOW_NAME"]
        self.hermes_vars = ['HERMES_ADAPTER_MODE', 'HERMES_CLIENT_CONF', 'HERMES_CONF']

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
                'msg': 'Name of the conda environment to update parameters (if multiple, separate by comma)',
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
            },
            {
                'name': 'taskname_file_path',
                'msg': 'The path to the file to store the task names',
                'type': str,
                'default': None,
            },
            {
                'name': 'with_hermes',
                'msg': 'Whether to run Hermes with DayuTracker',
                'type': bool,
                'default': False,
            },

        ]
    
    def _unset_conda_vars(self,env_vars_toset):
        
        # deliminate by comma
        conda_envs = self.config['conda_env'].split(',')
        
        for cenv in conda_envs:
            # TODO: change to use empty string
            cmd = ['conda', 'env', 'config', 'vars', 'unset',]
            
            for env_var in env_vars_toset:
                cmd.append(f'{env_var}')
            cmd.append('-n')
            cmd.append(cenv)
            
            cmd = ' '.join(cmd)
            Exec(cmd, LocalExecInfo(env=self.mod_env,))
            self.log(f"DayuTracker _unset_vfd_vars: {cmd}")
    
    def _set_conda_vars(self,env_vars_toset):
        
        # deliminate by comma
        conda_envs = self.config['conda_env'].split(',')
        
        for cenv in conda_envs:
            # Get current environment variables
            cmd = [ 'conda', 'env', 'config', 'vars', 'set']
            for env_var in env_vars_toset:
                env_var_val = self.env[env_var]
                if env_var_val:
                    cmd.append(f'{env_var}={env_var_val}')
            
            cmd.append('-n')
            cmd.append(cenv)
            cmd = ' '.join(cmd)
            Exec(cmd, LocalExecInfo(env=self.mod_env,))
            self.log(f"DayuTracker _set_env_vars: {cmd}")
            
    
    def _setup_vfd_tracker(self):
        """
        Set up the VFD tracker environment variables.

        :return: None
        """
        self.log(f"DayuTracker Setting up VFD tracker environment variables.")
        
        # Set current environment variables
        self.setenv('HDF5_DRIVER', self.vfd_tracker_name)
        self.log(f"DayuTracker HDF5_DRIVER: \"{self.env['HDF5_DRIVER']}\"")
        
        driver_config_str = f'\"{self.config["stat_file_path"]};{str(self.config["tracker_page_size"])}\"'
        
        self.setenv('HDF5_DRIVER_CONFIG', driver_config_str)
        self.log(f"DayuTracker HDF5_DRIVER_CONFIG: {self.env['HDF5_DRIVER_CONFIG']}")
        
        # if not empty, append the current HDF5_PLUGIN_PATH to the new plugin path
        if self.config['vol_tracker']:
            plugin_path_str = f'\"{self.config["dayu_lib"]}/vfd/:{self.config["dayu_lib"]}/vol/\"'
        else:
            plugin_path_str = f'\"{self.config["dayu_lib"]}/vfd/\"'
        self.setenv('HDF5_PLUGIN_PATH', plugin_path_str)
        self.log(f"DayuTracker HDF5_PLUGIN_PATH: {self.env['HDF5_PLUGIN_PATH']}")
    
    def _setup_hermes_vars(self):
        
        for vars in self.hermes_vars:
            vars_val = self.env[vars]
            self.setenv(vars, vars_val)
            self.log(f"DayuTracker {vars}: {self.env[vars]}")

    
    def _setup_vol_tracker(self):
        """
        Set up the volume tracker environment variables.

        :return: None
        """
        self.log(f"DayuTracker Setting up VOL tracker environment variables.")
        
        vol_connector_str = (
            f'\"{self.vol_tracker_name} under_vol=0;'
            + 'under_info={};'
            + f'path={self.config["stat_file_path"]};'
            + 'level=2;format=\"'
            )
        
        self.log(f"DayuTracker HDF5_VOL_CONNECTOR: {vol_connector_str}")
        
        self.setenv('HDF5_VOL_CONNECTOR', vol_connector_str)
        
        # if not empty, append the current HDF5_PLUGIN_PATH to the new plugin path
        if self.config['vfd_tracker']:
            plugin_path_str = f'\"{self.config["dayu_lib"]}/vol/:{self.config["dayu_lib"]}/vfd/\"'
        else:
            plugin_path_str = f'\"{self.config["dayu_lib"]}/vol/\"'
        
        self.setenv('HDF5_PLUGIN_PATH', plugin_path_str)
        self.log(f"DayuTracker HDF5_PLUGIN_PATH: {self.env['HDF5_PLUGIN_PATH']}")
    
    def _configure(self, **kwargs):
        """
        Converts the Jarvis configuration to application-specific configuration.
        E.g., OrangeFS produces an orangefs.xml file.

        :param kwargs: Configuration parameters for this pkg.
        :return: None
        """


        # workflow name is required
        if not self.config['workflow_name']:
            raise ValueError("workflow_name is required")
        else:
            # modify task file to be unique for each workflow
            self.config['taskname_file_path'] = "/tmp/" + self.config['workflow_name']
            self.setenv('PATH_FOR_TASK_FILES', self.config['taskname_file_path'])
            self.setenv('WORKFLOW_NAME', self.config['workflow_name'])

        if self.config['vfd_tracker']:
            self._setup_vfd_tracker()
        if self.config['vol_tracker']:
            self._setup_vol_tracker()
        
        if self.config['with_hermes']:
            self._setup_hermes_vars()

    def modify_env(self):
        """
        Modify the jarvis environment.

        :return: None
        """

        # check if stat_file_path exist, if not make it
        pathlib.Path(self.config['stat_file_path']).mkdir(parents=True, exist_ok=True)

        # Make path when start
        pathlib.Path(self.config['taskname_file_path']).mkdir(parents=True, exist_ok=True)
        
        
        # Set conda environment variables if required
        if self.config['conda_env'] is not None:
            conda_env_to_set = self.dayu_task_vars
        
            if self.config['vfd_tracker']: conda_env_to_set.extend(self.vfd_env_vars)
            if self.config['vol_tracker']: conda_env_to_set.extend(self.vol_env_vars)
            if self.config['with_hermes']: conda_env_to_set.extend(self.hermes_vars)
            
            conda_env_to_set = list(set(conda_env_to_set))
            self._unset_conda_vars(conda_env_to_set)
            self._set_conda_vars(conda_env_to_set)