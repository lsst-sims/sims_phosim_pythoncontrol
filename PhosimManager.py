#!/usr/bin/python

"""Classes for managing phosim execution."""

from __future__ import with_statement
import ConfigParser
import functools
import glob
import logging
import os
import shutil
import subprocess
import sys
import time
import zipfile

import Exposure
import PhosimUtil
import ScriptWriter
import phosim

__author__ = 'Jeff Gardner (gardnerj@phys.washington.edu)'

logger = logging.getLogger(__name__)

MANIFEST_FN = 'manifest.txt'

@property
def NotImplementedField(self):
  raise NotImplementedError

def ObservationIdFromTrimfile(instance_catalog, extra_commands=None):
  """Returns observation ID and filter_num as read from instance_catalog."""
  obsid = None
  for line in open(instance_catalog, 'r'):
    if line.startswith('Opsim_obshistid'):
      obsid = line.strip().split()[1]
    elif line.startswith('Opsim_filter'):
      filter_num = line.strip().split()[1]
  assert obsid
  if extra_commands:
    for line in open(extra_commands, 'r'):
      if line.startswith('extraid'):
        obsid += line.split()[1]
  return obsid, filter_num

class PhosimManager(object):
  """Parent class for managing Phosim execution on distributed platforms.

  This is intended to be a base class for stage-specific implementations
  (e.g. preprocessor, raytrace).  Note that  _InitOutputDirectories() is
  needs to be implemented in each subclass.
  """

  def __init__(self, policy, manifest_parser_class=PhosimUtil.ManifestParser):
    """Constructor.

    Args:
      policy:  ConfigParser object to python_control config file.
    """
    self.policy = policy
    self.scratch_exec_path = self.policy.get('general', 'scratch_exec_path')
    self.save_path = self.policy.get('general','save_path')
    self.stage_path = self.policy.get('general','stage_path')
    self.use_shared_datadir = self.policy.getboolean('general','use_shared_datadir')
    self.shared_data_path = self.policy.get('general', 'shared_data_path')
    self.data_tarball = self.policy.get('general', 'data_tarball')
    self.debug_level = self.policy.getint('general','debug_level')
    self.python_exec = self.policy.get('general', 'python_exec')
    self.python_control_dir = self.policy.get('general', 'python_control_dir')
    self.phosim_bin_dir = self.policy.get('general', 'phosim_binDir')
    self.manifest_parser_class = manifest_parser_class
    # The following should be defined in subclasses
    self.phosim_data_dir = NotImplementedField
    self.phosim_output_dir = NotImplementedField
    self.phosim_work_dir = NotImplementedField

  def InitDirectories(self):
    """Initializes execution working directories and moves input files.

    Default implementations are provided for _InitExecDirectories() and
    _MoveInputFiles(), but subclasses must implement _InitOutputDirectories().
    """
    logger.info('Entering InitDirectories().')
    self._InitExecDirectories()
    self._MoveInputFiles()
    self._InitOutputDirectories()

  def UpdatePhosimDirsInPars(self, pars_path,
                             dirs_to_update=['datadir', 'instrdir', 'seddir']):
    """Moves pars file to temp file and rewrites it with new dirs.

    One problem in wrapping the phosim execution environment the way we do
    is that phosim actually stores the names of directories in may of its
    .pars files.  This is problematic if we run one stage (e.g. raytracing)
    in a different location than another (e.g. preprocessing).  Use this method
    to update all instances of 'dirs_to_update' encountered in the .pars
    file 'pars_path'.

    Args:
      pars_path:      Name of pars file to rewrite, including path.
      dirs_to_update: A list of dirs to update.  Possibilities are:
                      bindir, datadir, instrdir, outputdir, seddir
    """
    pars_dir, pars_fn = os.path.split(pars_path)
    tmp_path = os.path.join(pars_dir, '_tmp.pars')
    shutil.move(pars_path, tmp_path)
    with open(tmp_path, 'r') as pars_in:
      with open(pars_path, 'w') as pars_out:
        for line in pars_in:
          if 'bindir' in dirs_to_update and line.startswith('bindir'):
            pars_out.write('bindir %s\n' % self.phosim_bin_dir)
          elif 'datadir' in dirs_to_update and line.startswith('datadir'):
            pars_out.write('datadir %s\n' % self.phosim_data_dir)
          elif 'instrdir' in dirs_to_update and line.startswith('instrdir'):
            pars_out.write('instrdir %s\n' % self.phosim_instr_dir)
          elif 'outputdir' in dirs_to_update and line.startswith('outputdir'):
            pars_out.write('outputdir %s\n' % self.phosim_output_dir)
          elif 'seddir' in dirs_to_update and line.startswith('seddir'):
            pars_out.write('seddir %s\n' % os.path.join(self.phosim_data_dir, 'SEDs'))
          else:
            pars_out.write(line)
    os.remove(tmp_path)

  def Cleanup(self):
    """Clean up execution directory."""
    if os.path.exists(self.phosim_work_dir):
      PhosimUtil.RemoveDirOrLink(self.phosim_work_dir)
    if os.path.exists(self.phosim_output_dir):
      PhosimUtil.RemoveDirOrLink(self.phosim_output_dir)
    if os.path.exists(self.phosim_data_dir):
      PhosimUtil.RemoveDirOrLink(self.phosim_data_dir)

  def _BuildDataDir(self):
    """Makes a symlink to shared_data_path or unarchives data_tarball."""
    assert not os.path.exists(self.phosim_data_dir)
    if self.use_shared_datadir:
      if not os.path.isdir(self.shared_data_path):
        raise RuntimeError('shared_data_path %s does not exist.' %
                           self.shared_data_path)
      logger.info('_BuildDataDir() linking %s to %s.', self.shared_data_path,
                  self.phosim_data_dir)
      os.symlink(self.shared_data_path, self.phosim_data_dir)
    else:
      os.makedirs(self.phosim_data_dir)
      tarball_path = os.path.join(self.shared_data_path, self.data_tarball)
      if not os.path.isfile(tarball_path):
        raise RuntimeError('Data tarball %s does not exist.' % tarball_path)
      cmd = 'tar -xf %s -C %s' % (tarball_path, self.phosim_data_dir)
      logger.info('_BuildDataDir() executing %s' % cmd)
      subprocess.check_call(cmd, shell=True)

  def _InitExecDirectories(self):
    """Initializes directories needed for phosim execution."""
    if not os.path.isdir(self.my_exec_path):
      os.makedirs(self.my_exec_path)
    PhosimUtil.ResetDirectory(self.phosim_work_dir)
    PhosimUtil.ResetDirectory(self.phosim_output_dir)
    if os.path.exists(self.phosim_data_dir):
      PhosimUtil.RemoveDirOrLink(self.phosim_data_dir)

  def _InitOutputDirectories(self):
    """Deletes and recreates shared output directories."""
    raise NotImplementedError('_InitOutputDirectories() must be'
                              ' implemented subclass.')

  def _MoveInputFiles(self):
    """Manages any input files/data needed for phosim execution."""
    self._BuildDataDir()


class Preprocessor(PhosimManager):
  """Manages Phosim preprocessing stage.

  A typical use of an instance of this class is a single preprocessing
  run as follows:
    __init__():    Determines observation_id by reading trimfile and
                   extra_commands file.  Intialized ScriptWriter class for
                   writing raytrace shell scripts.
    InitExecEnvironment(): Calls InitDirectories() and initializes
                           PhosimFocalplane object.
    DoPreprocessing():  Does preprocessing step.  Output is written to
                        phosim_output_dir.
    ArchiveRaytraceInputByExt(): Archives preprocessing output before staging.
    StageOutput(): Copies output to 'stage_path'.
    Cleanup():     Cleans up 'scratch_exec_path'.
  """

  def __init__(self, imsim_config_file, instance_catalog,
               extra_commands=None, instrument='lsst', sensor='all',
               run_e2adc=True,
               script_writer_class=ScriptWriter.RaytraceScriptWriter):
    """Constructor.

    Args:
      policy:  ConfigParser object to python_control config file.
      imsim_config_file: Python_control config file.
      instance_catalog:  Full path to instance_catalog/trimfile
      extra_commands:    Full path to extra_commands/extraid file.
      instrument:        'lsst', 'subaru', etc.
      sensor:            'all', or string of sensor ID delimited by '|'.
      run_e2adc:         Run e2adc step after raytrace?
      script_writer_class: Class for writing the raytrace shell script.
                           Use this generate scheduler-specific scripts.

    Returns:
      Pointer to script_writer_class instance.
    """
    policy = ConfigParser.RawConfigParser()
    policy.read(imsim_config_file)
    PhosimManager.__init__(self, policy)

    self.imsim_config_file = imsim_config_file
    self.instance_catalog = instance_catalog.strip()
    self.extra_commands = extra_commands
    self.instrument = instrument
    self.sensor = sensor

    self.run_e2adc = run_e2adc
    self.observation_id, self.filter_num = ObservationIdFromTrimfile(
      instance_catalog, extra_commands=extra_commands)
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
    staged_config_file = os.path.join(self.my_output_path,
                                      os.path.basename(self.imsim_config_file))
    self.script_writer = script_writer_class(
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
    print os.getcwd()
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
    manifest_fn = os.path.join(self.my_output_path, MANIFEST_FN)
    logger.info('Writing exposures to %s', manifest_fn)
    with self.manifest_parser_class(manifest_fn, 'w') as manifest_parser:
      manifest_parser.Write([('param', 'observation_id', self.observation_id),
                             ('param', 'filter_num', self.filter_num),
                             ('param', 'instrument', self.instrument),
                             ('param', 'exec_script_base', exec_script_base),
                             ('param', 'pars_archive_name', pars_archive_name),
                             ('param', 'run_e2adc', self.run_e2adc),
                             ('param', 'skip_atmoscreens', skip_atmoscreens)])
      self.script_writer.SetExtraWriteOp(functools.partial(
        self._AppendExposureId, manifest_parser))
      PhosimUtil.RunWithWallTimer(
        functools.partial(self.focalplane.ScheduleRaytrace, self.instrument, self.run_e2adc),
        name=name)
    logger.info('Closed %s', manifest_fn)
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
    archives.append(self._ArchiveParsByExt(self.pars_archive_name,
                                           self.skip_atmoscreens))
    archives.extend(self._ArchiveExecScriptsByExt(exec_archive_name))
    archives.append(self.imsim_config_file)
    os.chdir(self.my_exec_path)
    return archives

  def StageOutput(self, fn_list, manifest_name='filemanifest.txt'):
    """Moves files to stage_path.

    Args:
      fn_list:  Name of files to move.
      manifest_name: Name of file containing list of output files.

    Raises:
      OSError upon failure of move or mkdir ops
    """
    manifest = []
    with self.manifest_parser_class(os.path.join(self.my_output_path, MANIFEST_FN), 'a') as parser:
      for fn in fn_list:
        manifest.append(('file', parser.ManifestFileTypeByExt(fn),
                         os.path.basename(fn)))
      parser.Write(manifest)
    logger.info('Staging output files to %s: %s', self.my_output_path, fn_list)
    PhosimUtil.StageFiles(fn_list, self.my_output_path)
    return


  def _AppendExposureId(self, parser, exposure_id):
    parser.Write([('set', 'exposure_id', exposure_id)])

  def _ArchiveParsByExt(self, archive_name, skip_atmoscreens):
    """Archives raytrace .pars files.

    Args:
      archive_name:  Name of archive for exec scripts.  If this ends
                     with '.txt', will simply write a manifest of exec
                     scripts and stage each exec file separately.
      skip_atmoscreens:  If True, the atmosphere generation step was skipped.

    Returns:
      Absolute path of archive file that was created.
    TODO(gardnerj): Add capability to handle .txt extension
    """
    # If we are regenerating atmosphere screens, all of the parameters
    # needed for this should be in raytrace_*.pars.
    globs = 'raytrace_*.pars tracking_*.pars e2adc_*.pars'
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
      archive_name:  Name of archive for exec scripts.  If this ends
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

class Raytracer(PhosimManager):
  """Manages Phosim raytracing stage.

  PhosimRaytracer can accomodate the nonexistance of atmosphere screens.
  If they do not exist, it simply runs phosim.GenerateAtmosphere().

  A typical use of an instance of this class is a single raytracing
  run as follows:
    __init__():     Inits class vars.
    InitExecEnvironment(): Calls InitDirectories() generates atmosphere
                           screens if needed.
    DoRaytracing(): Does preprocessing step.  Output is written to
                    phosim_output_dir.
    CopyOutput():   Copies output to 'save_path'.
    Cleanup():      Cleans up 'scratch_exec_path'.
    """

  def __init__(self, policy, observation_id, cid, eid,
               filter_num=None, instrument=None, run_e2adc=None,
               stdout_log_fn=None):
    """Constructor.

    If filter_num, instrument, or run_e2adc are None,
    the constructor will read these from the manifest, assumed to be
    located in stage_path/manifest.txt.

    Args:
      policy:  ConfigParser object to python_control config file.
      observation_id: ImSim/PhoSim observation ID.
      cid:            Chip ID.
      eid:            Exposure ID.
      filter_num:     Numeric identifier for filter.
      instrument:     'lsst', 'subaru', etc.
      run_e2adc:      Run e2adc step after raytrace?
      stdout_log_fn:  Name of file to which to write phosim stdout. None
                      writes stdout to stdout.
    """
    PhosimManager.__init__(self, policy)
    self.cid = cid
    self.eid = eid
    self.observation_id = observation_id
    self.fid = phosim.BuildFid(self.observation_id, self.cid, self.eid)
    self.filter_num = filter_num
    self.instrument = instrument
    self.run_e2adc = run_e2adc
    self.stdout_log_fn = stdout_log_fn
    # Directory from which to grab input files
    self.my_input_path = os.path.join(self.stage_path, self.observation_id)
    self._ReadParamsFromManifestIfNeeded() # Needs my_input_path
    # Directory in which to execute this instance.
    self.my_exec_path = os.path.join(self.scratch_exec_path, self.fid)
    # Phosim execution environment
    self.phosim_data_dir = os.path.join(self.my_exec_path, 'data')
    self.phosim_output_dir = os.path.join(self.my_exec_path, 'output')
    self.phosim_work_dir = os.path.join(self.my_exec_path, 'work')
    self.phosim_instr_dir = os.path.join(self.phosim_data_dir, instrument)

  def _ReadParamsFromManifestIfNeeded(self):
    if (self.observation_id is None or self.filter_num is None or
        self.instrument is None or self.run_e2adc is None):
      with self.manifest_parser_class(os.path.join(self.my_input_path, MANIFEST_FN), 'r') as parser:
        parser.Read()
        obsid = parser.GetLastByTags('param', 'observation_id')
        if obsid != self.observation_id:
          logging.critical('Observation ID in manifest (%s) does not match this'
                           ' class instance (%s)', obsid, self.observation_id)
          raise RuntimeError('Observation ID in manifest (%s) does not match this'
                             ' class instance (%s)' % (obsid, self.observation_id))
        if not self.filter_num:
          self.filter_num = parser.GetLastByTags('param', 'filter_num')
        if not self.instrument:
          self.instrument = parser.GetLastByTags('param', 'instrument')
        if self.run_e2adc is None:
          self.run_e2adc = True if parser.GetLastByTags('param', 'run_e2adc') == 'True' else False

  def InitExecEnvironment(self, pars_archive_name='pars.zip',
                          skip_atmoscreens=False):
    """Initializes the execution environment.

    Creates working directories and constructs PhosimFocalplane instance.
    CHANGES DIRECTORY TO my_exec_path.

    Args:
      pars_archive_name: Name of pars archive (no path).
      skip_atmoscreens:  If True, the atmosphere generation step was skipped.
    """
    self.pars_archive_name=pars_archive_name
    self.InitDirectories()
    os.chdir(self.my_exec_path)
    self.CheckAndDoAtmoscreens()

  def CheckAndDoAtmoscreens(self):
    """Checks for and generates atmosphere screen output if needed.

    Returns:
      True of atmoscreens existed, False if they had to be recalculated."""
    if os.path.isfile(os.path.join(self.phosim_work_dir,
                                   'airglowscreen_%s.fits' % self.observation_id)):
      atmoscreen_glob = 'atmospherescreen_%s_*.fits' % self.observation_id
      if len(glob.glob(os.path.join(self.phosim_work_dir, atmoscreen_glob))) == 70:
        logger.info('Found existing airglowscreen and atmospherescreen files.'
                    ' Skipping atmosphere step.')
        return True
    logger.info('Could not find existing airglowscreen and atmospherescreen files.'
                ' Running PhosimFocalplane.GenerateAtmosphere().')
    os.chdir(self.phosim_work_dir)
    focalplane = phosim.PhosimFocalplane(self.my_exec_path,
                                         self.phosim_output_dir,
                                         self.phosim_work_dir,
                                         self.phosim_bin_dir,
                                         self.phosim_data_dir,
                                         self.phosim_instr_dir,
                                         grid='cluster',
                                         grid_opts={})
    # Set input file for GenerateAtmosphere step:
    focalplane.inputParams = os.path.basename(self.my_raytrace_pars)
    focalplane.GenerateAtmosphere()
    os.chdir(self.my_exec_path)
    return False

  def DoRaytrace(self, raytrace_func=phosim.jobchip):
    """Perform raytrace step.

    Args:
      raytrace_func:  Function that performs the raytracing.  Takes arguments
                      like phosim.jobchip().
    """
    os.chdir(self.phosim_work_dir)
    # Redirect stdout into a log file.
    # http://stackoverflow.com/questions/4675728/redirect-stdout-to-a-file-in-python
    if self.stdout_log_fn:
      logger.info('Redirecting raytrace stdout to %s.', self.stdout_log_fn)
      old = os.dup(1)
      os.close(1)
      os.open(self.stdout_log_fn, os.O_WRONLY|os.O_APPEND)
    logger.info('Calling %s(%s, %s, %s, %s, %s, %s, %s, instrument=%s run_e2adc=%s)',
                raytrace_func.__name__, self.observation_id, self.cid, self.eid,
                self.filter_num, self.phosim_output_dir, self.phosim_bin_dir,
                self.phosim_data_dir, self.instrument, self.run_e2adc)
    raytrace_func(self.observation_id, self.cid, self.eid, self.filter_num,
                  self.phosim_output_dir, self.phosim_bin_dir, self.phosim_data_dir,
                  instrument=self.instrument, run_e2adc=self.run_e2adc)
    sys.stdout.flush()
    # Un-redirect stdout
    if self.stdout_log_fn:
      os.close(1)
      os.dup(old)
      os.close(old)
    os.chdir(self.my_exec_path)

  def CopyOutput(self, zip_rawfiles=False):
    """Copies output for save_path.

    Args:
      zip_rawfiles: Archive the e2adc output files for this exposure into
                    a single zip file?
    """
    os.chdir(self.phosim_output_dir)
    exposure = Exposure.Exposure(self.observation_id,
                                 Exposure.filterToLetter(self.filter_num),
                                 '%s_%s' % (self.cid, self.eid))
    dest_path, dest_fn = exposure.generateEimageOutputName()
    dest_path = self._PrependAndCreateFullSavePath(dest_path)
    src_fn = exposure.generateEimageExecName()
    logger.info('Copying %s to %s.', os.path.join(self.phosim_output_dir, src_fn),
                os.path.join(dest_path, dest_fn))
    shutil.copy(src_fn, os.path.join(dest_path, dest_fn))
    if self.run_e2adc:
      amp_list = self._LoadAmpList()
      if zip_rawfiles or self.policy.getboolean('general', 'zip_rawfiles'):
        self._CopyZippedRawOutput(exposure, amp_list)
      else:
        self._CopyRawOutput(exposure, amp_list)

  def _LoadAmpList(self):
    with open(os.path.join(self.phosim_instr_dir,
                           'segmentation.txt'), 'r') as ampf:
      logger.info('Reading amp_list from %s.', ampf.name)
      amp_list = Exposure.readAmpList(ampf, self.cid)
    return amp_list

  def _CopyAndModifyParsFiles(self):
    """Copy .pars files to phosim_work_dir and append proper dirs.

    After copy, corrects the values of seddir, datadir, and instrdir.

    Raises:
      OSError if file operation fails.
      CalledProcessError if unarchive fails.
    """
    cmd = 'unzip -d %s %s' % (
      self.phosim_work_dir, os.path.join(self.my_input_path, self.pars_archive_name))
    if self.stdout_log_fn:
      cmd += ' >> %s' % self.stdout_log_fn
    logger.info('Executing %s' % cmd)
    subprocess.check_call(cmd, shell=True)
    self.my_raytrace_pars = os.path.join(self.phosim_work_dir,
                                         'raytrace_%s.pars' % self.fid)
    self.my_e2adc_pars = os.path.join(self.phosim_work_dir,
                                      'e2adc_%s.pars' % self.fid)
    if os.path.isfile(self.my_raytrace_pars):
      logger.info('Updating directories in %s' % self.my_raytrace_pars)
      self.UpdatePhosimDirsInPars(self.my_raytrace_pars)
    else:
      raise OSError('Could not find file %s.' % self.my_raytrace_pars)
    if os.path.isfile(self.my_e2adc_pars):
      logger.info('Updating directories in %s' % self.my_e2adc_pars)
      self.UpdatePhosimDirsInPars(self.my_e2adc_pars)
    else:
      self.my_e2adc_pars = None

  def _MoveInputFiles(self):
    """Manages any input files/data needed for phosim execution."""
    self._BuildDataDir()
    self._CopyAndModifyParsFiles()

  def _InitOutputDirectories(self):
    if not os.path.exists(self.save_path):
      os.makedirs(self.save_path)

  def _PrependAndCreateFullSavePath(self, dest_dir):
    """Prepends self.save_dir to dest_dir and creates it if necessary.

    Returns:
      os.path.join(self.save_path, dest_dir)
    """
    dest_path = os.path.join(self.save_path, dest_dir)
    if not os.path.exists(dest_path):
      logger.info('Creating %s.', dest_path)
      os.makedirs(dest_path)
    return dest_path

  def _CopyRawOutput(self, exposure, amp_list):
    """Copies e2adc output from phosim_output_dir to proper dir in save_path."""
    dest_path, dest_fns = exposure.generateRawOutputNames(ampList=amp_list)
    dest_path = self._PrependAndCreateFullSavePath(dest_path)
    src_fns = exposure.generateRawExecNames(ampList=amp_list)
    for src_fn, dest_fn in zip(src_fns, dest_fns):
      logger.info('Copying %s to %s.', os.path.join(self.phosim_output_dir, src_fn),
                  os.path.join(dest_path, dest_fn))
      shutil.copy(src_fn, os.path.join(dest_path, dest_fn))

  def _CopyZippedRawOutput(self, exposure, amp_list):
    """Copies e2adc output from phosim_output_dir to zip in proper dir in save_path."""
    dest_path, dest_fns = exposure.generateRawOutputNames(ampList=amp_list)
    dest_path = self._PrependAndCreateFullSavePath(dest_path)
    src_fns = exposure.generateRawExecNames(ampList=amp_list)
    zip_name = os.path.join(dest_path, PhosimUtil.ZipNameFromRaw(dest_fns[0]))
    zipf = zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_STORED)
    for src_fn, dest_fn in zip(src_fns, dest_fns):
      src = os.path.join(self.phosim_output_dir, src_fn)
      logger.info('Adding %s as %s to %s.', src, dest_fn, zip_name)
      zipf.write(src, dest_fn)
    zipf.close()
