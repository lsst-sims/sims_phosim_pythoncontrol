#!/usr/bin/python

"""Classes for writing Imsim/PhoSim execution scripts."""

from __future__ import with_statement
import datetime
import getpass
import logging
import os
import stat

import phosim

__author__ = 'Jeff Gardner (gardnerj@phys.washington.edu)'

logger = logging.getLogger(__name__)


class ScriptWriter(object):
  """Writes scripts for various ImSim/PhoSim stages."""
  def __init__(self, phosim_bin_dir, phosim_data_dir, phosim_output_dir,
               phosim_work_dir, debug_level=0, python_exec='python',
               python_control_dir='.', imsim_config_file=None,
               exec_script_base=None, pars_archive_fullpath=None,
               extra_write_op=None):
    """Constructor.

    Args:
      exec_script_base:  Base for written exec scripts.  Instance-specific
                         tags will be appended after this (e.g. exposure_id).
      extra_write_op:    Execute this op at the end of every call to WriteScript()
                         Takes a single string as argument.
    """
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
    self._extra_write_op = extra_write_op

  def SetExecScriptBase(self, exec_script_base):
    self._exec_script_base = exec_script_base

  def GetExecScriptBase(self):
    return self._exec_script_base

  def SetParsArchive(self, name):
    self._pars_archive_fullpath = name

  def GetParsArchive(self):
    return self._pars_archive_fullpath

  def SetExtraWriteOp(self, name):
    self._extra_write_op = name

  def GetExtraWriteOp(self):
    return self._extra_write_op


class RaytraceScriptWriter(ScriptWriter):
  """ScriptWriter for running raytrace stage.

  Subclass this for scheduler-specific implementations.
  """

  def WriteScript(self, observation_id, cid, eid, filter_num, output_dir,
                  bin_dir, data_dir, instrument='lsst', run_e2adc=True):
    """Write the actual script.

    This is designed to be arg-compatible with phosim.jobchip().

    Args:
      observation_id: ImSim/PhoSim observation ID.
      cid:            Chip ID.
      eid:            Exposure ID.
      filter_num:     Numeric identifier for filter.
      output_dir:     phosim_output_dir
      bin_dir:        phosim_bin_dir
      data_dir:       phosim_data_dir
      instrument:     'lsst', 'subaru', etc.
      run_e2adc:      Run e2adc step after raytrace?
    """
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
    self._ChmodPlusX(script_name)
    if self._extra_write_op:
      # Write cid_eid to manifest file.
      self._extra_write_op('%s_%s' % (cid, eid))
    return

  def _ChmodPlusX(self, fn):
    """Does a 'chmod a+x' to fn."""
    st = os.stat(fn)
    os.chmod(fn, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

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
    outf.write('### Note: You may add additional arguments to the onechip.py\n'
               '###       command line through $1\n')
    outf.write('### ---------------------------------------\n\n')
    outf.write('if ($#argv == 1) then\n'
               '   set extra_args = "$1"\n'
               'else\n'
               '   set extra_args = ""\n'
               'endif\n')
    outf.write('# Set PYTHONPATH in order to import phosim.py correctly.\n'
               'if ($?PYTHONPATH) then\n'
               '   setenv PYTHONPATH %s:{$PYTHONPATH}\n'
               'else\n'
               '   setenv PYTHONPATH %s\n'
               'endif\n' % (self.phosim_bin_dir, self.phosim_bin_dir))
    cmd = ('%s %s %s %s %s %s %s --instrument=%s' %
           (self.python_exec, os.path.join(self.python_control_dir, 'onechip.py'),
            self.imsim_config_file, observation_id, cid, eid, filter_num,
            instrument))
    if not run_e2adc:
      cmd += ' --no_e2adc'
    if self._pars_archive_fullpath:
      cmd += ' --pars_archive=%s' % self._pars_archive_fullpath
    cmd += ' $extra_args'
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


class PbsRaytraceScriptWriter(RaytraceScriptWriter):
  """An example subclass for writing PBS scripts instead of csh scripts.

  This is a skeleton example of how one would implement their own
  scheduler-specific script writer.
  """

  def ParsePbsConfig(self, policy):
    """Parses PBS-specific variables from config file.

    Add more variables to the config file under the 'pbs' section
    and you can read them in here.

    Args:
      policy:  ConfigParser object to python_control config file.
    """
    self.policy = policy
    assert self.policy.get('general','scheduler2') == 'pbs'
    self.email = self.policy.get('pbs','email')
    self.job_name = self.policy.get('pbs','job_name')
    self.n_cores = self.policy.get('pbs', 'cores_per_node')
    self.walltime = self.policy.get('pbs', 'walltime')

  def _WriteHeader(self, outf, observation_id, cid, eid, filter_num,
                   instrument, run_e2adc):
    """Write PBS-specific header."""
    log_fn = os.path.join(self.policy.get('general', 'log_dir'),
                          observation_id,
                          'fullFocalplane_%s_stdout.log' % observation_id)
    logger.info('Generating Raytrace PBS script header.')
    outf.write('#!/bin/csh')
    outf.write(' -x\n') if self.debug_level >= 2 else outf.write('\n')
    outf.write('### -------------------------------------------------------------\n')
    outf.write('### PBS   script created by: %s\n' % getpass.getuser())
    outf.write('###              created on: %s\n' % str(datetime.datetime.now()))
    outf.write('### Observation ID:          %s\n' % observation_id)
    outf.write('### Chip ID:                 %s\n' % cid)
    outf.write('### Exposure ID:             %s\n' % eid)
    outf.write('### Instrument:              %s\n' % instrument)
    outf.write('### -------------------------------------------------------------\n\n')
    outf.write('#PBS -N %s\n' % self.job_name)
    outf.write('#PBS -M %s\n' % self.email)
    outf.write('#PBS -j oe\n')
    outf.write('#PBS -m a\n')
    outf.write('#PBS -o %s\n' % log_fn)
    outf.write('#PBS -l walltime=%s\n' % self.walltime)
    outf.write('#PBS -l nodes=1:ppn=%s\n' % self.n_cores)
    outf.write('\n')
