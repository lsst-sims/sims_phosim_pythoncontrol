#!/usr/bin/python
from __future__ import with_statement
import datetime
import getpass
import logging
import os

import phosim2 as phosim

logger = logging.getLogger(__name__)


class ScriptWriter(object):
  """Writes scripts for various ImSim/PhoSim stages."""
  def __init__(self, phosim_bin_dir, phosim_data_dir, phosim_output_dir,
               phosim_work_dir, debug_level=0, python_exec='python',
               python_control_dir='.', imsim_config_file=None,
               exec_script_base=None, pars_archive_fullpath=None):
    self.phosim_bin_dir = phosim_bin_dir
    self.phosim_data_dir = phosim_data_dir
    self.phosim_output_dir = phosim_output_dir
    self.phosim_work_dir = phosim_work_dir
    self.debug_level = debug_level
    self.python_exec = python_exec
    self.python_control_dir = python_control_dir
    self.imsim_config_file = imsim_config_file
    self._exec_script_base = exec_script_base
    self._pars_archive_fullpath = pars_archive_fullpath

  def SetExecScriptBase(self, exec_script_base):
    self._exec_script_base = exec_script_base

  def GetExecScriptBase(self):
    return self._exec_script_base

  def SetParsArchive(self, name):
    self._pars_archive_fullpath = name

  def GetParsArchive(self):
    return self.pars_archive_fullpath


class RaytraceScriptWriter(ScriptWriter):
  """ScriptWriter for running raytrace stage."""

  def _WriteHeader(self, outf, observation_id, cid, eid, filter_num, instrument,
                           run_e2adc):
    logger.info('Generating Raytrace script header.')
    outf.write('#!/bin/csh')
    outf.write(' -x\n') if self.debug_level >= 2 else outf.write('\n')
    outf.write('### -------------------------------------------------------------\n')
    outf.write('### Shell script created by: %s\n' % getpass.getuser())
    outf.write('###              created on: %s\n' % str(datetime.datetime.now()))
    outf.write('### Observation ID:          %s\n' % observation_id)
    outf.write('### Chip ID:                 %s\n' % cid)
    outf.write('### Exposure ID:             %s\n' % eid)
    outf.write('### Instrument:              %s\n' % instrument)
    outf.write('### -------------------------------------------------------------\n\n')

  def _WriteStageIn(self, outf, observation_id, cid, eid, filter_num, instrument,
                    run_e2adc):
    """Interface for manual stage-in commands.

    Currently, the stage-in is handled by the same raytrace exec python script.
    """
    outf.write('### ---------------------------------------\n')
    outf.write('### Input Stage-in Section\n')
    outf.write('### ---------------------------------------\n\n')

  def _WriteExec(self, outf, observation_id, cid, eid, filter_num, instrument,
                 run_e2adc):
    logger.info('Generating Raytrace script exec section.')
    outf.write('### ---------------------------------------\n')
    outf.write('### Executable Section\n')
    outf.write('### ---------------------------------------\n\n')
    outf.write('# Set PYTHONPATH in order to import phosim.py correctly.\n')
    outf.write('if ($?PYTHONPATH) then\n')
    outf.write('   setenv PYTHONPATH %s:{$PYTHONPATH}\n' % self.phosim_bin_dir)
    outf.write('else\n')
    outf.write('   setenv PYTHONPATH %s\n' % self.phosim_bin_dir)
    outf.write('endif\n')
    cmd = ('%s %s %s %s %s %s %s --instrument=%s' %
           (self.python_exec, os.path.join(self.python_control_dir, 'onechip.py'),
            self.imsim_config_file, observation_id, cid, eid, filter_num,
            instrument))
    if not run_e2adc:
      cmd += ' --no_e2adc'
    if self._pars_archive_fullpath:
      cmd += ' --pars_archive=%s' % self._pars_archive_fullpath
    logger.info('Script Exec command: %s', cmd)
    outf.write('%s\n\n' % cmd)

  def _WriteStageOut(self, outf, observation_id, cid, eid, filter_num, instrument,
                     run_e2adc):
    """Interface for manual stage-out commands.

    Currently, the stage-out is handled by the same raytrace exec python script.
    """
    outf.write('### ---------------------------------------\n')
    outf.write('### Input Stage-out Section\n')
    outf.write('### ---------------------------------------\n\n')

  def WriteScript(self, observation_id, cid, eid, filter_num, output_dir,
                  bin_dir, data_dir, instrument='lsst', run_e2adc=True):
    assert self._exec_script_base
    assert output_dir == self.phosim_output_dir
    assert bin_dir == self.phosim_bin_dir
    assert data_dir == self.phosim_data_dir
    script_name = '%s_%s.csh' % (self._exec_script_base, phosim.BuildFid(observation_id, cid, eid))
    logger.info('Generating raytrace script %s', script_name)
    with open(script_name, 'w') as outf:
      self._WriteHeader(outf, observation_id, cid, eid, filter_num, instrument, run_e2adc)
      self._WriteStageIn(outf, observation_id, cid, eid, filter_num, instrument, run_e2adc)
      self._WriteExec(outf, observation_id, cid, eid, filter_num, instrument, run_e2adc)
      self._WriteStageOut(outf, observation_id, cid, eid, filter_num, instrument, run_e2adc)
    return
