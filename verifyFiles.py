#!/usr/bin/python

"""Verifies file as various stages of the ImSim workflow.

Only for ImSim/PhoSim versions <= 3.0.x!

Date:    May 11, 2012
Author:  Jeff Gardner, U. Washington, gardnerj@phys.washington.edu
Updated:

Usage:   python verifyFiles.py [options] obshistid filter path
Options: -s, --stage:  workflow stage to check:
                         input: input files for raytrace
                         exec_output: raytrace output files on exec node
                         final_output: raytrace output files in shared storage
         obshistid:    obshistid
         filter:       letter filter ID
         path:         Path to the data, excluding any <obshistid>-f<filter> info

Example: To verify input files for obshistid=871731611, filter=z, stagePath2=/share/gardnerj/staging2:
             python verifyFiles.py --stage input 871731611 z /share/gardnerj/staging2
         (The files in question are actually in /share/gardnerj/staging2/871731611-fz/run871731611)
"""

from __future__ import with_statement
import sys
import optparse
from Focalplane import *
from Exposure import *
from chip import WithTimer

def WriteErrors(output_stream, missing_list, corrupt_list):
  add_to_returncode = 0
  if len(missing_list) > 0 or len(corrupt_list) > 0:
    output_stream.write('WARNING: verifyFiles returned errors:\n')
    if len(missing_list) > 0:
      for f in missing_list:
        output_stream.write('-- FileNotFound: %s\n' %f)
      add_to_returncode += 2
    if len(corrupt_list) > 0:
      for f,e in corrupt_list:
        output_stream.write('-- FitsverifyFailure: %s\n' %e)
      add_to_returncode += 4
  return add_to_returncode

def main(stage, obshistid, filterid, path, id_list, exp_list, camstr, outfilename, no_stderr):
  missing_list = []
  corrupt_list = []
  fp = Focalplane(obshistid, filterid)
  if stage == 'raytrace_input':
    missing_list = fp.verifyInputFiles(path, id_list)
  elif stage == 'raytrace_exec':
    if not id_list or not len(id_list)==1:
      raise RuntimeError("IDLIST must be set to exactly 1 id in raytrace_exec mode")
    exposure = Exposure(obshistid, filterid, id_list[0])
    missing_list, corrupt_list = exposure.verifyExecFiles(path)
  elif stage == 'raytrace_output':
    if not id_list:
      for cidTuple in fp.generateCidList(camstr, id_list):
        for expid in exp_list:
          id_list.append('%s_%s' %(cidTuple[0], expid))
    for id in id_list:
      exposure = Exposure(obshistid, filterid, id)
      missing_list.extend(exposure.verifyOutputFiles(path))
  else:
    sys.stderr.write('Unrecognized stage: %s\n' %stage)
    return 1
  if not no_stderr:
    returncode = WriteErrors(sys.stderr, missing_list, corrupt_list)
  if outfilename:
    with open(outfilename, 'w') as f:
      returncode = WriteErrors(f, missing_list, corrupt_list)
  return returncode


if __name__ == "__main__":

  usage = "usage: %prog [options] obshistid filter path"
  parser = optparse.OptionParser(usage=usage)
  parser.add_option("-s", "--stage", dest="stage",
                    help="Stage to check: raytrace_input, raytrace_exec, raytrace_output (default)",
                    default="raytrace_output")
  parser.add_option("-i", "--idlist", dest="idlist", default="",
                    help="(optional) comma-separated list of ids (Rxx_Sxx_Exxx) to process." +
                         "  Overrides EXPLIST.")
  parser.add_option("-e", "--explist", dest="explist", default="E000,E001",
                    help="If IDLIST not specified, a comma-separated list of exposures to process" +
                         " in stages 'raytrace_exec' and 'raytrace_output'" +
                         "('raytrace_input' will determine this from the exec_* files)")
  parser.add_option("-c", "--camstr", dest="camstr", default="Group0",
                    help="If IDLIST not specified, a regex string for which camera group to use" +
                         " (see fullFocalPlane script and SIM_CAMCONFIG for more details)")
  parser.add_option("-o", "--output", dest="outfilename", default="",
                    help="Optionally output errors to file of name OUTFILENAME")
  parser.add_option("-n", "--no_stderr", dest="no_stderr", action="store_true",
                    default=False, help="Do not output to stderr (default is to" +
                    " output to stderr and OUTFILENAME if specified)")
  (options, args) = parser.parse_args()

  if len(args) != 3:
    parser.print_help()
    sys.exit(1)
  obshistid = args[0]
  filterid = args[1]
  path = args[2]
  id_list = []
  if options.idlist:
    id_list = options.idlist.split(",")
  if not options.explist:
    raise RuntimeError("Unrecognized explist")
  exp_list = options.explist.split(",")
  result = main(options.stage, obshistid, filterid, path, id_list, exp_list, options.camstr,
                options.outfilename, options.no_stderr)
  sys.exit(result)
        
