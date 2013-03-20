#!/usr/bin/python2.6
import os
import tempfile
import unittest
import PhosimUtil

def MakeTmpDir():
  return tempfile.mkdtemp()


class ManifestParserTest(unittest.TestCase):
  def setUp(self):
    self.tmpdir = MakeTmpDir()
    self.manifest_fn = os.path.join(self.tmpdir, '_Mock_manifest.txt')
    with PhosimUtil.ManifestParser(self.manifest_fn, 'w') as parser:
      self.exec_files = [
        ('file', parser.ManifestFileTypeByExt('mock.csh'), 'mock.csh'),
        ('file', parser.ManifestFileTypeByExt('mock.pbs'), 'mock.pbs')
        ]
      self.archive_files = [
        ('file', parser.ManifestFileTypeByExt('mock.tar'), 'mock.tar'),
        ('file', parser.ManifestFileTypeByExt('mock.tgz'), 'mock.tgz'),
        ('file', parser.ManifestFileTypeByExt('mock.tar.gz'), 'mock.tar.gz'),
        ('file', parser.ManifestFileTypeByExt('mock.zip'), 'mock.zip')
        ]
      self.config_files = [
        ('file', parser.ManifestFileTypeByExt('mock.cfg'), 'mock.cfg')
        ]
      self.pars_files = [
        ('file', parser.ManifestFileTypeByExt('mock.pars'), 'mock.pars'),
        ('file', parser.ManifestFileTypeByExt('mock.pars.gz'), 'mock.pars.gz')
        ]
      self.observation_id = '12345'
      self.filter_num = '1'
      self.instrument = 'fake_telescope'
      self.params = [
        ('param', 'observation_id', self.observation_id),
        ('param', 'filter_num', self.filter_num),
        ('param', 'instrument', self.instrument)
        ]
      self.manifest = (self.exec_files + self.archive_files + self.config_files +
                       self.pars_files + self.params)
      parser.Write(self.manifest)

  def testGetByAllTags(self):
    with PhosimUtil.ManifestParser(self.manifest_fn, 'r') as parser:
      parser.Read()
      self.assertEquals(parser.GetAllByTags('file', 'exec'),
                        map(lambda row: row[2], self.exec_files))
      self.assertEquals(parser.GetAllByTags('file', 'archive'),
                        map(lambda row: row[2], self.archive_files))
      self.assertEquals(parser.GetAllByTags('file', 'config'),
                        map(lambda row: row[2], self.config_files))
      self.assertEquals(parser.GetAllByTags('file', 'pars'),
                        map(lambda row: row[2], self.pars_files))

  def testGetByMajor(self):
    with PhosimUtil.ManifestParser(self.manifest_fn, 'r') as parser:
      parser.Read()
      self.assertEquals(parser.GetByMajor('param'),
                        self.params)

  def testGetLastByTags(self):
    parser = PhosimUtil.ManifestParser()
    parser.Open(self.manifest_fn, 'r')
    parser.Read()
    self.assertEquals(parser.GetLastByTags('file', 'pars'),
                      self.pars_files[-1][2])
    parser.Close()

if __name__ == '__main__':
    unittest.main()
