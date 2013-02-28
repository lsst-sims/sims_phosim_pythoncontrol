#!/usr/bin/python
from __future__ import with_statement
import functools
import glob
import logging
import os
import shutil
import sys
import time

import PhosimUtil
import ScriptWriter
import phosim2 as phosim

logger = logging.getLogger(__name__)

@property
def NotImplementedField(self):
  raise NotImplementedError

def ObservationIdFromTrimfile(instance_catalog, extra_commands=None):
  """Returns observation ID as read frim instance_catalog."""
  obsid = None
  for line in open(instance_catalog, 'r'):
    if line.startswith('Opsim_obshistid'):
      obsid = line.strip().split()[1]
  assert obsid
  if extra_commands:
    for line in open(extra_commands, 'r'):
      if line.startswith('extraid'):
        obsid += line.split()[1]
  return obsid

class PhosimManager(object):
  """Parent class for managing Phosim execution on distributed platforms."""

  def __init__(self, instance_catalog, policy, extra_commands=None):

    self.instance_catalog = instance_catalog.strip()
    self.policy = policy
    self.extra_commands = extra_commands
    self.scratch_exec_path = self.policy.get('general', 'scratch_exec_path')
    self.save_path = self.policy.get('general','save_path')
    self.stage_path = self.policy.get('general','stage_path')
    self.use_shared_datadir = self.policy.getboolean('general','use_shared_datadir')
    self.shared_data_path = self.policy.get('general', 'shared_data_path')
    self.data_tarball = self.policy.get('general', 'data_tarball')
    self.debug_level = self.policy.getint('general','debug_level')
    self.regen_atmoscreens = self.policy.getboolean('general','regen_atmoscreens')
    self.python_exec = self.policy.get('general', 'python_exec')
    self.python_control_dir = self.policy.get('general', 'python_control_dir')
    self.phosim_bin_dir = self.policy.get('general', 'phosim_binDir')
    # The following should be defined in subclasses
    self.phosim_data_dir = NotImplementedField
    self.phosim_output_dir = NotImplementedField
    self.phosim_work_dir = NotImplementedField

  def _InitExecDirectories(self):
    """Initializes directories needed for phosim execution."""
    if not os.path.isdir(self.my_exec_path):
      os.makedirs(self.my_exec_path)
    PhosimUtil.ResetDirectory(self.phosim_work_dir)
    PhosimUtil.ResetDirectory(self.phosim_output_dir)
    if os.path.exists(self.phosim_data_dir):
      PhosimUtil.RemoveDirOrLink(self.phosim_data_dir)

  def _BuildDataDir(self):
    """Makes a symlink to shared_data_path or unarchives data_tarball."""
    assert not os.path.exists(self.phosim_data_dir)
    if self.use_shared_datadir:
      if not os.path.isdir(self.shared_data_path):
        raise RuntimeError('shared_data_path %s does not exist.' %
                           self.shared_data_path)
      os.symlink(self.shared_data_path, self.phosim_data_dir)
    else:
      os.makedirs(self.phosim_data_dir)
      tarball_path = os.path.join(self.shared_data_path, self.data_tarball)
      if not os.path.isfile(tarball_path):
        raise RuntimeError('Data tarball %s does not exist.' % tarball_path)
      cmd = 'tar -xf %s -C %s' % (tarball_path, self.phosim_data_dir)
      logger.info('Executing %s' % cmd)
      subprocess.check_call(cmd, shell=True)

  def _MoveInputFiles(self):
    """Manages any input files/data needed for phosim execution."""
    self._BuildDataDir()

  def _InitOutputDirectories(self):
    """Deletes and recreates shared output directories."""
    raise NotImplementedError('_InitOutputDirectories() must be'
                              ' implemented subclass.')

  def InitDirectories(self):
    """Initializes execution working directories and moves input files."""
    self._InitExecDirectories()
    self._MoveInputFiles()
    self._InitOutputDirectories()

  def Cleanup(self):
    if os.path.exists(self.phosim_work_dir):
      PhosimUtil.RemoveDirOrLink(self.phosim_work_dir)
    if os.path.exists(self.phosim_output_dir):
      PhosimUtil.RemoveDirOrLink(self.phosim_output_dir)
    if os.path.exists(self.phosim_data_dir):
      PhosimUtil.RemoveDirOrLink(self.phosim_data_dir)



class PhosimPreprocessor(PhosimManager):
  """Manages Phosim preprocessing stage."""

  def __init__(self, instance_catalog, policy, extra_commands=None,
               instrument='lsst', sensor='all', run_e2adc=True):
    PhosimManager.__init__(self, instance_catalog, policy, extra_commands)

    self.instrument = instrument
    self.sensor = sensor

    self.run_e2adc = run_e2adc
    self.observation_id = ObservationIdFromTrimfile(instance_catalog)
    # Directory in which to execute this instance.
    self.my_exec_path = os.path.join(self.scratch_exec_path, self.observation_id)
    # Directory to which to copy preprocessing output upon completion.
    self.my_output_path = os.path.join(self.stage_path, self.observation_id)
    # Arguments for PhosimFocalplane
    self.phosim_data_dir = os.path.join(self.my_exec_path, 'data')
    self.phosim_output_dir = os.path.join(self.my_exec_path, 'output')
    self.phosim_work_dir = os.path.join(self.my_exec_path, 'work')
    self.phosim_instr_dir = os.path.join(self.phosim_data_dir, instrument)
    self.focalplane = None
    self.script_writer = ScriptWriter.RaytraceScriptWriter(
      self.phosim_bin_dir, self.phosim_data_dir, self.phosim_output_dir,
      self.phosim_work_dir, debug_level=self.debug_level,
      python_exec=self.python_exec, python_control_dir=self.python_control_dir)


  def InitExecEnvironment(self):
    """Initializes the execution environment.

    Creates working directories and constructs PhosimFocalplane instance.
    CHANGES DIRECTORY TO my_exec_path.
    """
    self.InitDirectories()
    grid_opts = {'script_writer': self.script_writer.WriteScript,
                 'submitter': None}
    logger.info('Creating instance PhosimFocalplane(%s, %s, %s, %s, %s, %s,'
                 ' grid=%s, grid_opts=%s', self.my_exec_path, self.phosim_output_dir,
                 self.phosim_work_dir, self.phosim_bin_dir, self.phosim_data_dir,
                 self.phosim_instr_dir, 'cluster', grid_opts)
    self.focalplane = phosim.PhosimFocalplane(self.my_exec_path,
                                              self.phosim_output_dir,
                                              self.phosim_work_dir,
                                              self.phosim_bin_dir,
                                              self.phosim_data_dir,
                                              self.phosim_instr_dir,
                                              grid='cluster',
                                              grid_opts=grid_opts)
    os.chdir(self.my_exec_path)


  def _InitOutputDirectories(self):
    PhosimUtil.ResetDirectory(self.my_output_path)

  def DoPreprocessing(self, skip_atmoscreens=False, log_timings=True,
                      exec_script_base='exec_raytrace'):
    """Performs phosim preprocessing stage and generates scripts for raytrace.

    Args:
      skip_atmoscreens:  If True, will skip the step for generating atmosphere
                         screens.  This is useful in distributed environments
                         where the extra computation time is cheaper than the
                         bandwidth required to transfer the screens to the
                         raytrace workers.
      log_timings:       Logs execution time of each of the steps.

    Returns:
      True upon success, False otherwise.

    TODO(gardnerj): Modify phosim so that it returns success/failure.
    """
    logger.info('Calling LoadInstanceCatalog(%s, %s).', self.instance_catalog,
                 self.extra_commands)
    self.focalplane.LoadInstanceCatalog(self.instance_catalog, self.extra_commands)
    logger.info('self.observation_id: %s    self.focalplane.observationID: %s',
                 self.observation_id, self.focalplane.observationID)
    os.chdir(self.phosim_work_dir)
    name = 'WriteInputParamsAndCatalogs' if log_timings else None
    PhosimUtil.RunWithWallTimer(self.focalplane.WriteInputParamsAndCatalogs, name=name)
    if not skip_atmoscreens:
      name = 'GenerateAtmosphere' if log_timings else None
      PhosimUtil.RunWithWallTimer(self.focalplane.GenerateAtmosphere, name=name)
    name = 'GenerateInstrumentConfig' if log_timings else None
    PhosimUtil.RunWithWallTimer(self.focalplane.GenerateInstrumentConfig, name=name)
    name = 'GenerateTrimObjects' if log_timings else None
    PhosimUtil.RunWithWallTimer(
      functools.partial(self.focalplane.GenerateTrimObjects, self.sensor), name=name)
    name = 'ScheduleRaytrace' if log_timings else None
    self.script_writer.SetExecScriptBase(exec_script_base)
    PhosimUtil.RunWithWallTimer(
      functools.partial(self.focalplane.ScheduleRaytrace, self.instrument, self.run_e2adc),
      name=name)
    os.chdir(self.my_exec_path)
    return True

  def ArchiveRaytraceInputByExt(self, archive_name='pars.zip',
                                skip_atmoscreens=False):
    """Archives output from DoPreprocessing().

    Automatically selects proper archive method from file extension
    by using PhosimUtil.ArchiveFilesByExtAndDelete().

    Args:
      skip_atmoscreens:  If True, does not package atmosphere screens.

    Returns:
      A list of archives that were created with full paths.

    Raises:
      CalledProcessError if archive op fails.
    """
    os.chdir(self.phosim_work_dir)
    globs = 'raytrace_*.pars e2adc_*.pars'
    if not skip_atmoscreens:
      globs += ' *.fits *.fits.gz'
    archive_fullpath = PhosimUtil.ArchiveFilesByExtAndDelete(archive_name, globs)
    logger.info('Archived globs "%s" into "%s".', globs, archive_fullpath)
    os.chdir(self.my_exec_path)
    return [archive_fullpath]

  def ArchiveRaytraceScriptsByExt(self, archive_name=None,
                                  exec_manifest_name=None):
    """Archives raytrace exec scripts.

    For running in 'csh' environment, don't archive anything.

    Args:
      archive_name:  Ignored for this implementation.  Would be the name of
                     archive file.
      exec_manifest_name: Create a file of this name and write to it a manifest
                     of all of the exec files that were created.

    Returns:
      A list of absolute paths to all exec files, plus the exec_manifest file if
      it was created.
    """
    exec_script_base = self.script_writer.GetExecScriptBase()
    assert exec_script_base
    if archive_name:
      logger.warning('Ignoring archive_name=%s...I\'m not archiving anything.',
                     archive_name)
    exec_list = map(os.path.abspath,
                    glob.glob(os.path.join(self.phosim_work_dir,
                                           '%s_*.csh' % exec_script_base)))
    if exec_manifest_name:
      with open(os.path.join(self.phosim_work_dir,
                             exec_manifest_name), 'w') as exec_manifest:
        for script in exec_list:
          exec_manifest.write('%s\n' % os.path.basename(script))
      exec_list.append(os.path.join(self.phosim_work_dir, exec_manifest_name))
    return exec_list

  def StageOutput(self, fn_list):
    """Moves files to stage_path.

    Args:
      fn_list:  Name of files to move.

    Raises:
      OSError upon failure of move or mkdir ops
    """
    logger.info('Staging output files to %s: %s', self.my_output_path, fn_list)
    PhosimUtil.StageFiles(fn_list, self.my_output_path)
    return

class PhosimRaytracer(PhosimManager):
  # REMEMBER TO CAT THE atmosphere_<observation_id>.pars FILE
  # INTO raytrace_<fid>.pars IF ATMOSCREEN ARE GENERATED IN
  # RAYTRACING STAGE!
  pass


