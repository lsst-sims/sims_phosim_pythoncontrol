#!/usr/bin/python

"""Classes for verifying Phosim input and output."""

from __future__ import with_statement
import ConfigParser
import logging
import os
import zipfile

import PhosimManager
import PhosimUtil

__author__ = 'Jeff Gardner (gardnerj@phys.washington.edu)'

logger = logging.getLogger(__name__)


class PhosimVerifier(object):
  """Verifies Phosim input and/or output."""
  def __init__(self, imsim_config_file):
    self.policy = ConfigParser.RawConfigParser()
    self.policy.read(imsim_config_file)
    self.stage_path = self.policy.get('general','stage_path')

  def IsFile(self, fn):
    return os.path.isfile(fn)

  def Exists(self, fn):
    return os.path.exists(fn)

  def IsDir(self, fn):
    return os.path.isdir(fn)

  def _ReadZipIndex(self, zip_fn):
    # 'with' does not work with ZipFile in 2.5
    zipf = zipfile.ZipFile(zip_fn, 'r')
    contents = zipf.namelist()
    zipf.close()
    return contents


class PhosimPreprocVerifier(PhosimVerifier):
  """Verifies Phosim preprocessing stage."""
  def __init__(self, imsim_config_file, instance_catalog, extra_commands,
               my_output_path=None):
    PhosimVerifier.__init__(self, imsim_config_file)
    self.observation_id, self.filter_num = PhosimManager.ObservationIdFromTrimfile(
      instance_catalog, extra_commands=extra_commands)
    # Directory to which to copy preprocessing output upon completion.
    if my_output_path:
      self.my_output_path = my_output_path
    else:
      self.my_output_path = os.path.join(self.stage_path, self.observation_id)

  def VerifyOutput(self, manifest_fullpath=None):
    """Verifies preprocessing output in stage_dir.

    Args:
      manifest_fullpath:  Use alternate manifest.

    Returns:
      None upon success or list of missing files (this may not be a complete
      list of missing files).
    """
    if not manifest_fullpath:
      manifest_fullpath = os.path.join(self.my_output_path, PhosimManager.MANIFEST_FN)
    if not self.IsFile(manifest_fullpath):
      return [manifest_fullpath]
    with PhosimUtil.ManifestParser(manifest_fullpath, 'r') as parser:
      parser.Read()
      if parser.GetLastByTags('param', 'observation_id') != self.observation_id:
        raise RuntimeError('Manifest observation_id=%s does not equal %s.',
                           parser.GetLastByTags('param', 'observation_id'),
                           self.observation_id)
      exposure_ids = parser.GetAllByTags('param', 'exposure_id')
      logger.debug('VerifyOutput() found exposure_ids in %s: %s', manifest_fullpath,
                   exposure_ids)
      missing_list = self._VerifyExecScripts(parser, exposure_ids)
      missing_list.extend(self._VerifyParsFiles(parser, exposure_ids))
      missing_list.extend(self._VerifyConfigFiles(parser))
    return missing_list

  def _VerifyFileInListAndDir(self, fn, fn_list, dirname=None):
    if fn not in fn_list:
      return [fn]
    if dirname and not self.IsFile(os.path.join(dirname, fn)):
      return [os.path.join(dirname, fn)]
    return []

  def _VerifyConfigFiles(self, parser):
    logger.info('Verifying config file.')
    config_list = parser.GetAllByTags('file', 'config')
    if not config_list:
      return ['imsim_config_file']
    assert len(config_list) == 1
    if not self.IsFile(os.path.join(self.my_output_path, config_list[0])):
      return [os.path.join(self.my_output_path, config_list[0])]
    return []

  def _VerifyParsFiles(self, parser, exposure_ids):
    logger.info('Verifying pars files.')
    pars_archive_name = parser.GetLastByTags('param', 'pars_archive_name')
    pars_archive_path = os.path.join(self.my_output_path, pars_archive_name)
    if (pars_archive_name not in parser.GetAllByTags('file', 'archive') or
        not self.IsFile(pars_archive_path)):
      return [pars_archive_name]
    pars_list = self._ReadZipIndex(pars_archive_path)
    missing_files = []
    missing_files.extend(self._VerifyFileInListAndDir(
      'tracking_%s.pars' % self.observation_id, pars_list))
    if parser.GetLastByTags('param', 'skip_atmoscreens') != 'True':
      missing_files.extend(self._VerifyFileInListAndDir(
        'airglowscreen_%s.fits' % self.observation_id, pars_list))
      missing_files.extend(self._VerifyFileInListAndDir(
        'cloudscreen_%s_0.fits' % self.observation_id, pars_list))
      missing_files.extend(self._VerifyFileInListAndDir(
        'cloudscreen_%s_3.fits' % self.observation_id, pars_list))
      for i in range(7):
        missing_files.extend(self._VerifyFileInListAndDir(
          'atmospherecreen_%s_%d.fits' % (self.observation_id, i), pars_list))
    for exposure in exposure_ids:
      missing_files.extend(self._VerifyFileInListAndDir(
        'raytrace_%s_%s.pars' % (self.observation_id, exposure), pars_list))
      if parser.GetLastByTags('param', 'run_e2adc') == 'True':
        missing_files.extend(self._VerifyFileInListAndDir(
          'e2adc_%s_%s.pars' % (self.observation_id, exposure), pars_list))
    return missing_files


  def _VerifyExecScripts(self, parser, exposure_ids):
    logger.info('Verifying exec scripts.')
    exec_script_base = parser.GetLastByTags('param', 'exec_script_base')
    exec_files = parser.GetByMatcher(
      lambda row: row[2] if row[0] == 'file' and row[1] == 'exec' else None)
    missing_files = []
    for exposure in exposure_ids:
      exec_fn = '%s_%s_%s.csh' % (exec_script_base, self.observation_id,
                                  exposure)
      missing_files.extend(self._VerifyFileInListAndDir(exec_fn, exec_files,
                                                        dirname=self.my_output_path))
    return missing_files

