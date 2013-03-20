#!/usr/bin/python2.6
import os
import tempfile
import unittest
import ScriptWriter

class ScriptWriterTest(unittest.TestCase):

  def setUp(self):
    self.phosim_bin_dir = '_phosim_bin_dir'
    self.phosim_data_dir = '_phosim_data_dir'
    self.phosim_output_dir = '_phosim_output_dir'
    self.phosim_work_dir = '_phosim_work_dir'

  def testInit(self):
    writer = ScriptWriter.ScriptWriter(self.phosim_bin_dir,
                                       self.phosim_data_dir,
                                       self.phosim_output_dir,
                                       self.phosim_work_dir)
    self.assertEquals(self.phosim_bin_dir, writer.phosim_bin_dir)
    self.assertEquals(self.phosim_data_dir, writer.phosim_data_dir)
    self.assertEquals(self.phosim_output_dir, writer.phosim_output_dir)
    self.assertEquals(self.phosim_work_dir, writer.phosim_work_dir)


if __name__ == '__main__':
    unittest.main()
