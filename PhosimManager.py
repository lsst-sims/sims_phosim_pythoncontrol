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

  def __init__(self, policy):

    self.policy = policy
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

  def __init__(self, policy, imsim_config_file, instance_catalog,
               extra_commands=None, instrument='lsst', sensor='all',
               run_e2adc=True):
    PhosimManager.__init__(self, policy)

    self.imsim_config_file = imsim_config_file
    self.instance_catalog = instance_catalog.strip()
    self.extra_commands = extra_commands
    self.instrument = instrument
    self.sensor = sensor

    self.run_e2adc = run_e2adc
    self.observation_id = ObservationIdFromTrimfile(instance_catalog)
    # Directory in which to execute this instance.
    self.my_exec_path = os.path.join(self.scratch_exec_path, self.observation_id)
    # Directory to which to copy preprocessing output upon completion.
    self.my_output_path = os.path.join(self.stage_path, self.observation_id)
    # Phosim execution environment
    self.phosim_data_dir = os.path.join(self.my_exec_path, 'data')
    self.phosim_output_dir = os.path.join(self.my_exec_path, 'output')
    self.phosim_work_dir = os.path.join(self.my_exec_path, 'work')
    self.phosim_instr_dir = os.path.join(self.phosim_data_dir, instrument)
    self.focalplane = None
    self.pars_archive_name = None
    self.skip_atmoscreens = None
    staged_config_file = os.path.join(self.stage_path,
                                      os.path.basename(self.imsim_config_file))
    self.script_writer = ScriptWriter.RaytraceScriptWriter(
      self.phosim_bin_dir, self.phosim_data_dir, self.phosim_output_dir,
      self.phosim_work_dir, debug_level=self.debug_level,
      python_exec=self.python_exec, python_control_dir=self.python_control_dir,
      imsim_config_file=staged_config_file)


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
                      exec_script_base='exec_raytrace',
                      pars_archive_name=None):
    """Performs phosim preprocessing stage and generates scripts for raytrace.

    Args:
      skip_atmoscreens:  If True, will skip the step for generating atmosphere
                         screens.  This is useful in distributed environments
                         where the extra computation time is cheaper than the
                         bandwidth required to transfer the screens to the
                         raytrace workers.
      log_timings:       Logs execution time of each of the steps.
      exec_script_base:  The base of the raytrace exec scripts.
      pars_archive_name: Name of the archive to which raytrace pars files
                         will be written (this is needed by the ScriptWriter
                         in order to provide the raytrace script with the
                         proper command-line args).  If None, will be
                         set to 'pars_<observation_id>.zip'.

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
    if not pars_archive_name:
      pars_archive_name = 'pars_%s.zip' % self.focalplane.observationID
    self.pars_archive_name = pars_archive_name
    self.script_writer.SetParsArchive(self.pars_archive_name)
    self.skip_atmoscreens = skip_atmoscreens
    logger.debug('Set self.pars_archive_name=%s  self.skip_atmoscreens=%s',
                 self.pars_archive_name, self.skip_atmoscreens)
    PhosimUtil.RunWithWallTimer(
      functools.partial(self.focalplane.ScheduleRaytrace, self.instrument, self.run_e2adc),
      name=name)
    os.chdir(self.my_exec_path)
    return True

  def ArchiveRaytraceInputByExt(self, pars_archive_name=None,
                                exec_archive_name=None,
                                skip_atmoscreens=None):
    """Archives output from DoPreprocessing().

    Automatically selects proper archive method from file extension
    by using PhosimUtil.ArchiveFilesByExtAndDelete().

    Args:
      archive_name:       Override previous pars archive filename.
      exec_archive_name:  Name of archive for exec scripts.  If this ends
                          with '.txt', will simply write a manifest of exec
                          scripts and stage each exec file separately.
      skip_atmoscreens:   Override previous skip_atmoscreens.


    Returns:
      A list of archives that were created with full paths.

    Raises:
      CalledProcessError if archive op fails.
    """
    if pars_archive_name:
      self.pars_archive_name = pars_archive_name
    assert self.pars_archive_name
    if skip_atmoscreens is not None:
      self.skip_atmoscreens = skip_atmoscreens
    assert self.skip_atmoscreens is not None
    os.chdir(self.phosim_work_dir)
    archives = []
    archives.append(self._ArchiveParsByExt(pars_archive_name, skip_atmoscreens))
    archives.extend(self._ArchiveExecScriptsByExt(exec_archive_name))
    archives.append(self.imsim_config_file)
    os.chdir(self.my_exec_path)
    return archives

  def _ArchiveParsByExt(self, archive_name, skip_atmoscreens):
    """Archives raytrace .pars files.

    TODO(gardnerj): Add capability to handle .txt extension
    """
    # If we are regenerating atmosphere screens, all of the parameters
    # needed for this should be in raytrace_*.pars.
    globs = 'raytrace_*.pars e2adc_*.pars'
    if not self.skip_atmoscreens:
      globs += ' *.fits *.fits.gz'
    archive_fullpath = PhosimUtil.ArchiveFilesByExtAndDelete(self.pars_archive_name,
                                                             globs)
    logger.info('Archived globs "%s" into "%s".', globs, archive_fullpath)
    return archive_fullpath

  def _ArchiveExecScriptsByExt(self, archive_name='execmanifest_raytrace.txt'):
    """Archives raytrace exec scripts.

    For running in 'csh' environment, don't archive anything.

    Args:
      exec_archive_name:  Name of archive for exec scripts.  If this ends
                          with '.txt', will simply write a manifest of exec
                          scripts and stage each exec file separately.
    Returns:
      A list of absolute paths to all exec files, plus the exec_manifest file if
      it was created.
    TODO(gardnerj): Add capability to handle non-.txt extensions
    """
    if not archive_name.endswith('.txt'):
      raise NotImplementedError('Exec scripts to archive other than .txt'
                                ' has not been implemented, yet.')
    exec_script_base = self.script_writer.GetExecScriptBase()
    assert exec_script_base
    exec_list = map(os.path.abspath,
                    glob.glob(os.path.join(self.phosim_work_dir,
                                           '%s_*.csh' % exec_script_base)))
    with open(os.path.join(self.phosim_work_dir,
                           archive_name), 'w') as exec_manifest:
      for script in exec_list:
        exec_manifest.write('%s\n' % os.path.basename(script))
    exec_list.append(os.path.join(self.phosim_work_dir, archive_name))
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
  """Manages Phosim raytracing stage."""
  # REMEMBER TO CAT THE atmosphere_<observation_id>.pars FILE
  # INTO raytrace_<fid>.pars IF ATMOSCREEN ARE GENERATED IN
  # RAYTRACING STAGE!

  def __init__(self, policy, observation_id, cid, eid, filter_num,
               instrument='lsst', run_e2adc=True):
    PhosimManager.__init__(self, policy)
    self.observation_id = observation_id
    self.cid = cid
    self.eid = eid
    self.filter_num = filter_num
    self.instrument = instrument
    self.run_e2adc = run_e2adc
    self.fid = phosim.BuildFid(self.observation_id, self.cid, self.eid)
    # Directory in which to execute this instance.
    self.my_exec_path = os.path.join(self.scratch_exec_path, self.fid)
    # Phosim execution environment
    self.phosim_data_dir = os.path.join(self.my_exec_path, 'data')
    self.phosim_output_dir = os.path.join(self.my_exec_path, 'output')
    self.phosim_work_dir = os.path.join(self.my_exec_path, 'work')
    self.phosim_instr_dir = os.path.join(self.phosim_data_dir, instrument)

  def _CopyAndModifyParsFiles(self):
    """Copy .pars files to phosim_work_dir and append proper dirs.

    After copy, appends correct values of seddir, datadir, and instrdir.
    """
    shutil.copy(
                self.phosim_work_dir)
    cmd = 'unzip -d %s %s' % (self.phosim_work_dir,
                              os.path.join(self.stage_path,
                                         self.pars_archive_name))
    logger.info('Executing %s' % cmd)
    subprocess.check_call(cmd, shell=True)
    for pars_base in ['raytrace', 'e2adc']:
      pars_fn = '%s_%s.pars' % (pars_base, self.fid)
      if os.path.isfile(pars_fn):
        logger.info('Appending updated directories to file %s' % pars_fn)
        with open(pars_fn, 'a') as pars:
          pars.write('seddir %s\n' % os.path.join(self.phosim_data_dir, SEDs))
          pars.write('datadir %s\n' % self.phosim_data_dir)
          pars.write('instrdir %s\n' % self.phosim_instr_dir)

  def _MoveInputFiles(self):
    """Manages any input files/data needed for phosim execution."""
    self._BuildDataDir()
    self._CopyAndModifyParsFiles()

  def _InitOutputDirectories(self):
    pass

  def InitExecEnvironment(self, pars_archive_name='pars.zip'):
    """Initializes the execution environment.

    Creates working directories and constructs PhosimFocalplane instance.
    CHANGES DIRECTORY TO my_exec_path.
    """
    self.pars_archive_name=pars_archive_name
    self.InitDirectories()
    #os.chdir(self.my_exec_path)



