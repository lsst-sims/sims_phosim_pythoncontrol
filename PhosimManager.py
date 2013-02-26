#!/usr/bin/python
from __future__ import with_statement
import datetime
import functools
import getpass
import logging
import os
import shutil
import sys
import time

import PhosimUtil
import phosim2 as phosim

logger = logging.getLogger(__name__)

@property
def NotImplementedField(self):
  raise NotImplementedError

def ObservationIdFromTrimfile(instance_catalog):
  """Returns observation ID as read frim instance_catalog."""
  for line in open(instance_catalog, 'r'):
    if line.startswith('Opsim_obshistid'):
      return line.strip().split()[1]

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
    # The following should be defined in subclasses
    self.phosim_bin_dir = NotImplementedField
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
      if os.path.islink(self.phosim_data_dir):
        os.unlink(self.phosim_data_dir)
      else:
        shutil.rmtree(self.phosim_data_dir)

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
      logging.info('Executing %s' % cmd)
      subprocess.check_call(cmd, shell=True)

  def _MoveInputFiles(self):
    """Manages any input files/data needed for phosim execution."""
    self._BuildDataDir()

  def _InitOutputDirectories(self):
    """Deletes and recreates shared output directories."""
    raise NotImplementedError('_InitOutputDirectories() must be'
                              ' implemented subclass.')

  def InitDirectories(self):
    self._InitExecDirectories()
    self._MoveInputFiles()
    self._InitOutputDirectories()
    os.chdir(self.my_exec_path)


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
    self.phosim_bin_dir = self.policy.get('general', 'phosim_binDir')
    self.phosim_data_dir = os.path.join(self.my_exec_path, 'data')
    self.phosim_output_dir = os.path.join(self.my_exec_path, 'output')
    self.phosim_work_dir = os.path.join(self.my_exec_path, 'work')
    self.phosim_instr_dir = os.path.join(self.phosim_data_dir, instrument)

  def InitExecEnvironment(self):
    self.InitDirectories()
    grid_opts = {'script_writer': self.WriteRaytraceScript,
                 'submitter': None}
    logging.info('Creating instance PhosimFocalplane(%s, %s, %s, %s, %s, %s,'
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

  def _InitOutputDirectories(self):
    PhosimUtil.ResetDirectory(self.my_output_path)

  def WriteRaytraceScript(self, observation_id, cid, eid, filter_num, output_dir,
                          bin_dir, data_dir, instrument='lsst', run_e2adc=True):
    assert output_dir == self.phosim_output_dir
    assert bin_dir == self.phosim_bin_dir
    assert data_dir == self.phosim_data_dir
    assert self.instrument == instrument
    assert self.run_e2adc == run_e2adc
    script_name = 'exec_%s.csh' % phosim.BuildFid(observation_id, cid, eid)
    with open(script_name, 'w') as outf:
      outf.write('#!/bin/csh')
      outf.write(' -x\n') if self.debug_level else outf.write('\n')
      outf.write('### ---------------------------------------\n')
      outf.write('### Shell script created by: %s\n' % getpass.getuser())
      outf.write('###              created on: %s\n' % str(datetime.datetime.now()))
      outf.write('### observation ID:          %s\n' % observation_id)
      outf.write('### Chip ID:                 %s\n' % cid)
      outf.write('### Exposure ID:             %s\n' % eid)
      outf.write('### instrument:              %s\n' % instrument)
      outf.write('### ---------------------------------------\n\n')
    return

  def DoPreprocessing(self, skip_atmoscreens=False, log_timings=True):
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
    logging.info('Calling LoadInstanceCatalog(%s, %s).', self.instance_catalog,
                 self.extra_commands)
    self.focalplane.LoadInstanceCatalog(self.instance_catalog, self.extra_commands)
    logging.info('self.observation_id: %s    self.focalplane.observationID: %s',
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
    PhosimUtil.RunWithWallTimer(
      functools.partial(self.focalplane.ScheduleRaytrace, self.instrument, self.run_e2adc),
      name=name)
    os.chdir(self.my_exec_path)
    return True

  def ArchiveOutput(self, skip_atmoscreens=False,
                    trimcatalog_archive='trimcatalogs.tar.gz',
                    pars_archive='pars.tar.gz'):
    """Archives output from DoPreprocessing() into archives.

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
    archives = [self._ArchiveTrimcatalogs(trimcatalog_archive)]
    archives.append(self._ArchivePars(pars_archive, skip_atmoscreens=skip_atmoscreens))
    os.chdir(self.my_exec_path)
    return archives

  def _ArchiveTrimcatalogs(self, arc_fn):
    """Archive trimcatalog_*.pars separately becase they are so large."""
    logging.info('Archiving "trimcatalog_*.pars" files')
    return PhosimUtil.ArchiveFilesByExtAndDelete(arc_fn, 'trimcatalog_*.pars')

  def _ArchivePars(self, arc_fn, skip_atmoscreens=False):
    """Gathers .pars files into archive 'arc_fn'."""
    globs = '*.pars'
    if not skip_atmoscreens:
      globs += ' *.fits *.fits.gz'
    return PhosimUtil.ArchiveFilesByExtAndDelete(arc_fn, globs)

  def StageOutput(self, fn_list):
    """Moves files to stage_path.

    Args:
      fn_list:  Name of files to move.

    Raises:
      OSError upon failure of move or mkdir ops
    """
    logging.info('Staging output files to %s.', self.my_output_path)
    PhosimUtil.StageFiles(fn_list, self.my_output_path)
    return

class PhosimRaytracer(PhosimManager):
  # REMEMBER TO CAT THE atmosphere_<observation_id>.pars FILE
  # INTO raytrace_<fid>.pars IF ATMOSCREEN ARE GENERATED IN
  # RAYTRACING STAGE!
  pass


