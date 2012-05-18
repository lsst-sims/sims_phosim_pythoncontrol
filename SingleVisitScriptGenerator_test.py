#!/usr/bin/python2.6
import os
import unittest
from SingleVisitScriptGenerator import *
from optparse import OptionParser

class MockSingleVisitScriptGenerator(SingleVisitScriptGenerator):
  def _loadEnvironmentVars(self):
    self.imsimSourcePath ='/tmp'

  def _getImSimRevision(self):
    return "vTest"

class MockSingleVisitScriptGenerator_Pbs(SingleVisitScriptGenerator_Pbs):
  def _loadEnvironmentVars(self):
    self.imsimSourcePath ='/tmp'

  def _getImSimRevision(self):
    return "vTest"



class TestSingleVisitScriptGenerator(unittest.TestCase):

  def _SetupWorkstation(self):
    self.baseDir = os.path.dirname(__file__)
    self.imsimConfigFile = os.path.join(self.baseDir, 'imsimConfig_workstation.cfg')
    self.extraidFile = os.path.join(self.baseDir, 'clouds')
    self.scheduler = 'csh'
    self.policy = ConfigParser.RawConfigParser()
    self.policy.read(self.imsimConfigFile)

  def test_InitWorkstation(self):
    """
    Tests the import and initialization of SingleVisitScriptGenerator class.  Initialization
    includes parsing the .cfg file
    """
    self._SetupWorkstation()
    # This will raise an exception if it does not read in the config file
    s = MockSingleVisitScriptGenerator('/tmp', '_tmp.out', self.policy, self.imsimConfigFile,
                                       self.extraidFile, '_tmp.src.tgz', '_tmp.exec.tgz',
                                       '_tmp.control.tgz', '/tmp')

    # This variable will not be defined if config file not read in properly
    try:
      s.savePath
    except:
      raise
    self.assertEquals(s.imsimSourcePath, '/tmp')
    self.assertEquals(s.revision, 'vTest')

  def _SetupPbs(self):
    self._SetupWorkstation()
    self.imsimConfigFile = os.path.join(self.baseDir, 'imsimConfig_minerva.cfg')
    self.scheduler = 'pbs'
    self.policy = ConfigParser.RawConfigParser()
    self.policy.read(self.imsimConfigFile)

  def test_InitPbs(self):
    self._SetupPbs()
    # This will raise an exception if it does not read in the config file
    s = MockSingleVisitScriptGenerator_Pbs('/tmp', '_tmp.out', self.policy, self.imsimConfigFile,
                                           self.extraidFile, '_tmp.src.tgz', '_tmp.exec.tgz',
                                           '_tmp.control.tgz', '_tmp')

    # These variables will not be defined if config file not read in properly
    try:
      s.savePath
    except:
      raise
    try:
      s.username
    except:
      raise
    self.assertEquals(s.imsimSourcePath, '/tmp')
    self.assertEquals(s.revision, 'vTest')

if __name__ == '__main__':
    unittest.main()
