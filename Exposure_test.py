#!/usr/bin/python2.6
import os
import unittest
from Exposure import *

class MockExposure(Exposure):
  def _loadAmpList(self):
    self.ampList = [ 'C0' ]
    return

class TestExposure(unittest.TestCase):

  def setUp(self):
    self.e = MockExposure('12345678','r', 'R01_S00_E000')

  def test_Init(self):
    self.assertEqual(self.e.filterNum, '2')
    self.assertEqual(self.e.cid, 'R01_S00')
    return

  def test_generateEimageExecName(self):
    self.assertEqual(self.e.generateEimageExecName(), 'eimage_12345678_f2_R01_S00_E000.fits.gz')
    return

  def test_generateEimageOutputName(self):
    path, name = self.e.generateEimageOutputName()
    self.assertEqual(name, 'eimage_12345678_R01_S00_E000.fits.gz')
    self.assertEqual(path, 'eimage/v12345678-fr/E000/R01')
    return

  def test_generateRawExecNames(self):
    names = self.e.generateRawExecNames()
    self.assertTrue(len(names)==1)
    self.assertEqual(names[0], 'imsim_12345678_f2_C0_E000.fits.gz')
    return

  def test_generateRawOutputNames(self):
    path, names = self.e.generateRawOutputNames()
    self.assertTrue(len(names)==1)
    self.assertEqual(names[0], 'imsim_12345678_C0_E000.fits.gz')
    self.assertEqual(path, 'raw/v12345678-fr/E000/R01/S00')
    return

if __name__ == '__main__':
    unittest.main()
