#!/usr/bin/python

"""Classes for verifying Phosim input and output.

File access is aggresively abstracted to make it easier to
port to platforms with non-posix filesystems.
"""

from __future__ import with_statement
import ConfigParser
import logging
import os
import zipfile

import Exposure
import phosim
import PhosimManager
import PhosimUtil

__author__ = 'Jeff Gardner (gardnerj@phys.washington.edu)'

logger = logging.getLogger(__name__)


class PhosimVerifier(object):
  """Verifies Phosim input and/or output."""
  def __init__(self, imsim_config_file,
               manifest_parser_class=PhosimUtil.ManifestParser):
    self.policy = ConfigParser.RawConfigParser()
    self.policy.read(imsim_config_file)
    self.scratch_exec_path = self.policy.get('general', 'scratch_exec_path')
    self.stage_path = self.policy.get('general','stage_path')
    self.save_path = self.policy.get('general','save_path')
    self.manifest_parser_class = manifest_parser_class

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

  def _VerifyFileInDir(self, dirname, fn):
    if self.IsFile(os.path.join(dirname, fn)):
      return []
    logging.warning('Verification failure: File %s is not in directory %s.',
                    fn, dirname)
    return [os.path.join(dirname, fn)]

  def _VerifyFileInListAndDir(self, fn, fn_list, dirname=None):
    if fn not in fn_list:
      logging.warning('Verification failure: File %s is not in list %s.',
                      fn, fn_list)
      return [fn]
    if dirname:
      return self._VerifyFileInDir(dirname, fn)
    return []

  def _VerifyFitsInDir(self, dirname, fn):
    missing = self._VerifyFileInDir(dirname, fn)
    if missing:
      return missing
    corrupt_files = []
    if Exposure.verifyFitsContents(corrupt_files, dirname, fn):
      return []
    logging.error('verifyFitsContents failed %s with message %s',
                  corrupt_files[0], corrupt_files[1])
    return [corrupt_files[0]]



class RaytraceVerifier(PhosimVerifier):
  def __init__(self, imsim_config_file, observation_id, cid, eid,
               my_save_path=None, manifest_fullpath=None,
               manifest_parser_class=PhosimUtil.ManifestParser,
               instr_dir=None):
    """Constructor.

    Args:
      imsim_config_file: Python_control config file.
      observation_id: ImSim/PhoSim observation ID.
      cid:            Chip ID.
      eid:            Exposure ID.
      my_save_path:   Use path other than save_path in imsim_config_file.
      manifest_fullpath: Use file other than stage_path/manifest.txt
      manifest_parser_class: Use alternate class to parse manifest file.
      instr_dir:      Use path other than scratch_exec_path/fid/data/instrument.
    """
    PhosimVerifier.__init__(self, imsim_config_file, manifest_parser_class)
    self.observation_id = observation_id
    self.cid = cid
    self.eid = eid
    self.fid = phosim.BuildFid(self.observation_id, self.cid, self.eid)
    if manifest_fullpath:
      self.manifest_fullpath = manifest_fullpath
    else:
      self.manifest_fullpath = os.path.join(self.stage_path, self.observation_id,
                                            PhosimManager.MANIFEST_FN)
    self.my_save_path = my_save_path if my_save_path else self.save_path
    self.my_exec_path = os.path.join(self.scratch_exec_path, self.fid)
    self._LoadParamsFromManifest()
    self.phosim_data_dir = os.path.join(self.my_exec_path, 'data')
    self.phosim_output_dir = os.path.join(self.my_exec_path, 'output')
    self.phosim_instr_dir = (instr_dir if instr_dir else
                             os.path.join(self.phosim_data_dir, self.instrument))
    self.exposure = Exposure.Exposure(self.observation_id,
                                      Exposure.filterToLetter(self.filter_num),
                                      '%s_%s' % (self.cid, self.eid))
    logger.info('RaytraceVerifier instance created: imsim_config_file=%s'
                '  observation_id=%s  cid=%s  eid=%s  my_save_path=%s'
                '  my_exec_path=%s  phosim_instr_dir=%s  manifest_fullpath=%s'
                '  manifest_parser_class=%s', imsim_config_file, observation_id,
                cid, eid, self.my_save_path, self.my_exec_path, self.phosim_instr_dir,
                self.manifest_fullpath, self.manifest_parser_class.__name__)

  def _LoadParamsFromManifest(self):
    with self.manifest_parser_class(self.manifest_fullpath, 'r') as parser:
      parser.Read()
      obsid = parser.GetLastByTags('param', 'observation_id')
      if obsid != self.observation_id:
        logging.critical('Observation ID in manifest (%s) does not match this'
                         ' class instance (%s)', obsid, self.observation_id)
        raise RuntimeError('Observation ID in manifest (%s) does not match this'
                           ' class instance (%s)' % (obsid, self.observation_id))
      self.filter_num = parser.GetLastByTags('param', 'filter_num')
      self.instrument = parser.GetLastByTags('param', 'instrument')
      self.run_e2adc = True if parser.GetLastByTags('param', 'run_e2adc') == 'True' else False

  def VerifyScratchOutput(self):
    """Verifies raytrace output in phosim_output_dir.

    In addition to testing for existence, runs fitsverify on all .fits output.

    Returns:
      None upon success or list of missing or corrupt files.  fitsverify
      output is written to logging.ERROR.
    """
    logger.info('Verifying output files in %s.', self.phosim_output_dir)
    missing_files = self._VerifyFitsInDir(self.phosim_output_dir,
                                          self.exposure.generateEimageExecName())
    if self.run_e2adc:
      amp_list = self._LoadAmpList()
      raw_fns = self.exposure.generateRawExecNames(ampList=amp_list)
      for fn in raw_fns:
        missing_files.extend(self._VerifyFitsInDir(self.phosim_output_dir, fn))
    return missing_files

  def VerifySharedOutput(self):
    """Verifies raytrace output in save_path.

    Returns:
      None upon success or list of missing files (this may not be a complete
      list of missing files).
    """
    logger.info('Verifying output files in %s.', self.my_save_path)
    dest_path, dest_fn = self.exposure.generateEimageOutputName()
    missing_files = self._VerifyFileInDir(os.path.join(self.my_save_path, dest_path),
                                          dest_fn)
    if self.run_e2adc:
      amp_list = self._LoadAmpList()
      dest_path, dest_fns = self.exposure.generateRawOutputNames(ampList=amp_list)
      save_path = os.path.join(self.my_save_path, dest_path)
      zip_fullpath = os.path.join(save_path, PhosimUtil.ZipNameFromRaw(dest_fns[0]))
      if self.IsFile(zip_fullpath):
        missing_files = self._VerifyRawInZip(zip_fullpath, dest_fns)
      else:
        missing_files = self._VerifyRaw(save_path, dest_fns)
    return missing_files

  def _VerifyRawInZip(self, zip_fullpath, dest_fns):
    logger.info('Verifying e2adc output files in %s.', zip_fullpath)
    missing_files = []
    zip_index = self._ReadZipIndex(zip_fullpath)
    for fn in dest_fns:
      missing_files.extend(self._VerifyFileInListAndDir(fn, zip_index))
    return missing_files

  def _VerifyRaw(self, path, dest_fns):
    logger.info('Verifying e2adc output files in %s.', path)
    missing_files = []
    for fn in dest_fns:
      missing_files.extend(self._VerifyFileInDir(path, fn))
    return missing_files

  def _LoadAmpList(self):
    with open(os.path.join(self.phosim_instr_dir,
                           'segmentation.txt'), 'r') as ampf:
      logger.info('Reading amp_list from %s.', ampf.name)
      amp_list = Exposure.readAmpList(ampf, self.cid)
    return amp_list



class PreprocVerifier(PhosimVerifier):
  """Verifies Phosim preprocessing stage."""
  def __init__(self, imsim_config_file, instance_catalog, extra_commands,
               my_stage_path=None, manifest_parser_class=PhosimUtil.ManifestParser):
    PhosimVerifier.__init__(self, imsim_config_file, manifest_parser_class)
    self.observation_id, self.filter_num = PhosimManager.ObservationIdFromTrimfile(
      instance_catalog, extra_commands=extra_commands)
    # Directory to which to copy preprocessing output upon completion.
    if my_stage_path:
      self.my_output_path = my_stage_path
    else:
      self.my_output_path = os.path.join(self.stage_path, self.observation_id)
    logger.info('PreprocVerifier instance created: imsim_config_file=%s'
                '  instance_catalog=%s  extra_commands=%s  my_output_path=%s'
                '  observation_id=%s  filter_num=%s  manifest_parser_class=%s',
                imsim_config_file, instance_catalog, extra_commands,
                self.my_output_path, self.observation_id, self.filter_num,
                self.manifest_parser_class.__name__)

  def VerifySharedOutput(self, manifest_fullpath=None):
    """Verifies preprocessing output in stage_path.

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
    with self.manifest_parser_class(manifest_fullpath, 'r') as parser:
      parser.Read()
      if parser.GetLastByTags('param', 'observation_id') != self.observation_id:
        raise RuntimeError('Manifest observation_id=%s does not equal %s.',
                           parser.GetLastByTags('param', 'observation_id'),
                           self.observation_id)
      exposure_ids = parser.GetAllByTags('set', 'exposure_id')
      logger.debug('VerifyOutput() found exposure_ids in %s: %s', manifest_fullpath,
                   exposure_ids)
      missing_list = self._VerifyExecScripts(parser, exposure_ids)
      missing_list.extend(self._VerifyParsFiles(parser, exposure_ids))
      missing_list.extend(self._VerifyConfigFiles(parser))
    return missing_list

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
        for suffix in ['coarsex', 'coarsey', 'density_coarse', 'density_diff',
                       'density_fine', 'density_medium', 'finex', 'finey',
                       'mediumx', 'mediumy']:
          missing_files.extend(self._VerifyFileInListAndDir(
            'atmospherescreen_%s_%d_%s.fits' % (self.observation_id, i, suffix),
            pars_list))
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

