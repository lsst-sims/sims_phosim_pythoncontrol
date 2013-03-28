#!/usr/bin/python2.6
import ConfigParser
import os
import tempfile
import types
import unittest
import PhosimManager
import PhosimUtil
import ScriptWriter

def SetMockConfigContents(cfg, tmpdir, zip_rawfiles=True, scheduler2='csh'):
  """Sets mock config file vars and returns a dictionary of their settings."""
  cfg_dict = {
    'phosim_version': '_test_phosim_version',
    'python_exec': '_test_python_exec',
    'scheduler2': scheduler2,
    'debug_level': '2',
    'zip_rawfiles': zip_rawfiles,
    'log_stdout': True,
    'scratch_exec_path': os.path.join(tmpdir, 'exec'),
    'save_path': os.path.join(tmpdir, 'output'),
    'stage_path': os.path.join(tmpdir, 'staging'),
    'log_dir': os.path.join(tmpdir, 'logs'),
    'phosim_binDir': os.path.join(tmpdir, '_nonexistent'),
    'python_control_dir': os.path.join(tmpdir, '_nonexistent'),
    'use_shared_datadir': True,
    'shared_data_path': os.path.join(tmpdir, 'shared_data'),
    'data_tarball': os.path.join(tmpdir, '_nonexistent'),
    }

  cfg.add_section('general')
  for key, val in cfg_dict.iteritems():
    if type(val) == types.BooleanType:
      cfg.set('general', key, 'true' if val else 'false')
    else:
      cfg.set('general', key, val)
  return cfg_dict

def MakeTmpDir():
  return tempfile.mkdtemp()


class MockManifestParser(PhosimUtil.ManifestParser):
  pass

class MockScriptWriter(ScriptWriter.RaytraceScriptWriter):
  pass

class BasePhosimManagerTest(unittest.TestCase):
  """Base class with common setUp."""

  def GenerateMockConfigFile(self, config_fn, scheduler2='csh'):
    config = ConfigParser.RawConfigParser()
    cfg_dict = SetMockConfigContents(config, self.tmpdir, scheduler2=scheduler2)
    with open(config_fn, 'w') as cfg_f:
      config.write(cfg_f)
    return cfg_dict

  def GenerateMockExtraidFile(self, extraid_fn, extraid='3'):
    with open(extraid_fn, 'w') as eid_f:
      eid_f.write('extraid %s\n' % extraid)

  def GenerateMockTrimfile(self, trimfile_name, obs_hist_id='12345', filter_num='1',
                           extra_commands=None):
    with open(trimfile_name, 'w') as trimf:
      trimf.write('Opsim_obshistid %s\n' % obs_hist_id)
      trimf.write('Opsim_filter %s' % filter_num)

  def BaseSetup(self, scheduler2='csh'):
    self.tmpdir = MakeTmpDir()
    self.imsim_config_file = os.path.join(self.tmpdir, '_Mock_PhosimRaytracer.cfg')
    self.extraid_file = os.path.join(self.tmpdir, '_Mock_PhosimRaytracer_clouds')
    self.cfg_dict = self.GenerateMockConfigFile(self.imsim_config_file,
                                                scheduler2=scheduler2)
    self.extraid_file = self.GenerateMockExtraidFile(self.extraid_file)
    self.policy = ConfigParser.RawConfigParser()
    self.policy.read(self.imsim_config_file)


class PhosimManagerTest(BasePhosimManagerTest):

  def setUp(self):
    self.scheduler = 'csh'
    self.BaseSetup(scheduler2=self.scheduler)

  def testInit(self):
    mgr = PhosimManager.PhosimManager(
      self.policy, manifest_parser_class=MockManifestParser)
    self.assertEquals(self.cfg_dict['scratch_exec_path'], mgr.scratch_exec_path)
    self.assertEquals(self.cfg_dict['save_path'], mgr.save_path)
    self.assertEquals(self.cfg_dict['stage_path'], mgr.stage_path)
    self.assertEquals(self.cfg_dict['use_shared_datadir'], mgr.use_shared_datadir)

  def testUpdatePhosimDirsInPars(self):
    mgr = PhosimManager.PhosimManager(
      self.policy, manifest_parser_class=MockManifestParser)
    mgr.phosim_bin_dir = 'new_bindir'
    mgr.phosim_data_dir = 'new_datadir'
    mgr.phosim_instr_dir = 'new_instrdir'
    mgr.phosim_output_dir = 'new_outputdir'
    dir_values = {
      'bindir': ['old_bindir', mgr.phosim_bin_dir],
      'datadir': ['old_datadir', mgr.phosim_data_dir],
      'instrdir': ['old_instrdir', mgr.phosim_instr_dir],
      'outputdir': ['old_outputdir', mgr.phosim_output_dir],
      'seddir': ['old_seddir', os.path.join(mgr.phosim_data_dir,
                                            'SEDs')],
      }
    pars_path = os.path.join(self.tmpdir, '_test.pars')
    # update only bindir
    with open(pars_path, 'w') as parsf:
      for key, value in dir_values.iteritems():
        parsf.write('%s %s\n' % (key, value[0]))
    mgr.UpdatePhosimDirsInPars(pars_path, dirs_to_update=['bindir'])
    with open(pars_path, 'r') as parsf:
      for line in parsf:
        key, value = line.strip().split(' ', 1)
        if key == 'bindir':
          self.assertEquals(value, dir_values['bindir'][1])
        else:
          self.assertEquals(value, dir_values[key][0])
    os.remove(pars_path)
    # update everything
    with open(pars_path, 'w') as parsf:
      for key, value in dir_values.iteritems():
        parsf.write('%s %s\n' % (key, value[0]))
    mgr.UpdatePhosimDirsInPars(pars_path, dirs_to_update=['bindir', 'datadir',
                                                               'instrdir', 'outputdir',
                                                               'seddir'])
    with open(pars_path, 'r') as parsf:
      for line in parsf:
        key, value = line.strip().split(' ', 1)
        self.assertEquals(value, dir_values[key][1])
    os.remove(pars_path)

class PhosimPreprocessorTest(BasePhosimManagerTest):

  def setUp(self):
    self.scheduler = 'csh'
    self.BaseSetup(scheduler2=self.scheduler)
    self.trimfile = os.path.join(self.tmpdir, '_Mock_trimfile.dat')
    self.GenerateMockTrimfile(self.trimfile, obs_hist_id='12345', filter_num='1')

  def testInit(self):
    mgr = PhosimManager.Preprocessor(self.imsim_config_file, self.trimfile,
                                     self.extraid_file,
                                     script_writer_class=MockScriptWriter)
    self.assertEquals(self.cfg_dict['scratch_exec_path'], mgr.scratch_exec_path)
    self.assertEquals(self.cfg_dict['save_path'], mgr.save_path)
    self.assertEquals(self.cfg_dict['stage_path'], mgr.stage_path)
    self.assertEquals(self.cfg_dict['use_shared_datadir'], mgr.use_shared_datadir)
    self.assertEquals(mgr.observation_id, '12345')
    self.assertEquals(mgr.filter_num, '1')


if __name__ == '__main__':
    unittest.main()
