#!/usr/bin/python2.6
import os
import unittest
from AllVisitsScriptGenerator import *
from optparse import OptionParser


class MockAllVisitsScriptGenerator(AllVisitsScriptGenerator):
  def _loadTrimfileList(self, myfile):
    return 'mockTrimfile'

  def _loadEnvironmentVars(self):
    self.imsimSourcePath ='/tmp'
    self.imsimExecPath = '/tmp'

class MockAllVisitsScriptGenerator_Pbs(AllVisitsScriptGenerator_Pbs):
  def _loadTrimfileList(self, myfile):
    return 'mockTrimfile'

  def _loadEnvironmentVars(self):
    self.imsimSourcePath ='/tmp'
    self.imsimExecPath = '/tmp'



class TestAllVisitsScriptGenerator(unittest.TestCase):

  def _SetupWorkstation(self):
    self.baseDir = os.path.dirname(__file__)
    self.imsimConfigFile = os.path.join(self.baseDir, 'imsimConfig_workstation.cfg')
    self.extraidFile = os.path.join(self.baseDir, 'clouds')
    self.scheduler = 'csh'
    self.policy = ConfigParser.RawConfigParser()
    self.policy.read(self.imsimConfigFile)

  def test_InitWorkstation(self):
    """
    Tests the import and initialization of AllVisitsScriptGenerator class.  Initialization
    includes parsing the .cfg file
    """
    self._SetupWorkstation()
    # This will raise an exception if it does not read in the config file
    s = MockAllVisitsScriptGenerator('mockTrimFile', self.policy, self.imsimConfigFile,
                                     self.extraidFile)

    # This variable will not be defined if config file not read in properly
    try:
      s.savePath
    except:
      raise

  def _SetupPbs(self):
    self._SetupWorkstation()
    self.imsimConfigFile = os.path.join(self.baseDir, 'imsimConfig_minerva.cfg')
    self.scheduler = 'pbs'
    self.policy = ConfigParser.RawConfigParser()
    self.policy.read(self.imsimConfigFile)

  def test_InitPbs(self):
    self._SetupPbs()
    # This will raise an exception if it does not read in the config file
    s = MockAllVisitsScriptGenerator_Pbs('mockTrimFile', self.policy, self.imsimConfigFile,
                                         self.extraidFile)

    # These variables will not be defined if config file not read in properly
    try:
      s.savePath
    except:
      raise
    try:
      s.username
    except:
      raise

if __name__ == '__main__':
    unittest.main()
