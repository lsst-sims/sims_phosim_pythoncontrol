from __future__ import with_statement
import datetime
import functools
import logging
import os
import shutil
import sys
import time

import PhosimUtil
from phosim2 import PhosimFocalplane

logger = logging.getLogger(__name__)


class PhosimManager(object):
  """Parent class for managing Phosim execution on distributed platforms."""

  def __init__(self, instance_catalog, policy, extra_commands=None)

    self.instance_catalog = instance_catalog.strip()
    self.policy = policy
    self.extra_commands = extra_commands
    self.jobName = self.policy.get('general','jobname')
    self.exec_path = self.policy.get('general', 'scratchExecPath')
    self.scratch_output_dir = self.policy.get('general','scratchOutputDir')
    self.save_path = self.policy.get('general','savePath')
    self.stage_path = self.policy.get('general','stagePath1')
    self.stage_path2 = self.policy.get('general','stagePath2')
    self.use_shared_SEDs = self.policy.getboolean('general','useSharedSEDs')
    self.data_path_SEDs = self.policy.get('general', 'dataPathSEDs')
    self.debug_level = self.policy.getint('general','debuglevel')
    self.regen_atmoscreens = self.policy.getboolean('general','regenAtmoscreens')
    self.work_dir = os.path.join(self.exec_path, 'work')
    self.bin_dir = os.path.join(self.exec_path, 'bin')


class PhosimPreprocessor(PhosimManager):
  """Manages Phosim preprocessing stage."""

  def __init__(self, instance_catalog, policy, extra_commands=None,
               instrument='lsst', sensor='all', run_e2adc=True):
    PhosimManager.__init__(instance_catalog, policy, extra_commands)
    self.instrument = instrument
    self.sensor = sensor
    self.run_e2adc = run_e2adc
    self.instr_dir = os.path.join(self.data_path_SEDs, instrument)
    grid_opts = {'script_generator': self.WriteRaytraceScript,
                 'submitter': None}
    logging.info('Creating instance PhosimFocalplane(%s, %s, %s, %s, %s, %s,'
                 ' grid=%s, grid_opts=%s', self.exec_path, self.scratch_output_dir,
                 self.work_dir, self.bin_dir, self.data_path_SEDs, self.instr_dir,
                 'cluster', grid_opts)
    os.chdir(self.exec_dir)
    self.focalplane = PhosimFocalplane(self.exec_path, self.scratch_output_dir,
                                       self.work_dir, self.bin_dir,
                                       self.data_path_SEDs, self.instr_dir,
                                       grid='cluster', grid_opts=grid_opts)


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
    self.focalplane.LoadInstanceCatalog(self.instance_catalog, self.extra_commands)
    os.chdir(self.work_dir)
    if not skip_atmoscreens:
      name = 'GenerateAtmosphere' if log_timings else None
      PhosimUtil.RunWithTimer(focalplane.GenerateAtmosphere(), name=name)
    name = 'WriteInputParamsAndCatalogs' if log_timings else None
    PhosimUtil.RunWithTimer(focalplane.WriteInputParamsAndCatalogs(), name=name)
    name = 'GenerateInstrumentConfig' if log_timings else None
    PhosimUtil.RunWithTimer(focalplane.GenerateInstrumentConfig(), name=name)
    name = 'GenerateTrimObjects' if log_timings else None
    PhosimUtil.RunWithTimer(
      functools.partial(focalplane.GenerateTrimObjects, self.sensor), name=name)
    name = 'ScheduleRaytrace' if log_timings else None
    PhosimUtil.RunWithTimer(
      functools.partial(focalplane.ScheduleRaytrace, self.instrument, self.run_e2adc),
      name=name)
    os.chdir(self.exec_dir)
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
    os.chdir(self.work_dir)
    archives = [self._ArchiveTrimcatalogs(trimcatalog_archive)]
    archives.append(self._ArchivePars(pars_archive, skip_atmoscreens=skip_atmoscreens))
    os.chdir(self.exec_dir)
    return archives

  def _ArchiveTrimcatalogs(arc_fn):
    """Archive trimcatalog_*.pars separately becase they are so large."""
    logging.info('Archiving "trimcatalog_*.pars" files')
    return PhosimUtil.ArchiveFilesByExtAndDelete(arc_fn, 'trimcatalog_*.pars')

  def _ArchivePars(arc_fn, skip_atmoscreens=False):
    """Gathers .pars files into archive 'arc_fn'."""
    globs = '*.pars'
    if not skip_atmoscreens:
      globs += ' *.fits *.fits.gz'
    return PhosimUtil.ArchiveFilesByExtAndDelete(arc_fn, globs)

  def StageOutput(self, fn_list):
    """Moves files to stage_path2.

    Args:
      fn_list:  Name of files to move.

    Raises:
      OSError upon failure of move or mkdir ops
    """
    stage_dest = os.path.join(self.stage_path2, self.focalplane.observationID)
    logging.info('Staging output files to %s.', stage_dest)
    PhosimUtil.StageFiles(fn_list, stage_dest)
    return

