#!/usr/bin/python

"""
ADD DOCUMENTATION!
"""
from __future__ import with_statement
import ConfigParser
import logging
from optparse import OptionParser  # Can't use argparse yet, since we must work in 2.5
import os
import sys
import PhosimManager
import phosim2 as phosim

logger = logging.getLogger(__name__)


def ConfigureLogging(observation_id, fid, policy, log_to_stdout):
  if log_to_stdout:
    log_fn = None
  else:
    if policy.has_option('general', 'log_dir'):
      log_dir = os.path.join(policy.get('general', 'log_dir'), obsid)
      log_fn = os.path.join(log_dir, 'onechip_%s.log' % fid)
    else:
      log_fn = '/tmp/onechip.log'
  PhosimUtil.ConfigureLogging(policy.getint('general', 'debug_level'),
                              logfile_fullpath=log_fn)


def DoRaytrace(raytracer, pars_archive_name):
  raytracer.InitExecEnvironment(pars_archive_name=pars_archive_name)

def main(imsim_config_file, observation_id, cid, eid, filter_num,
         pars_archive_name='pars.zip', instrument='lsst', run_e2adc=True,
         log_to_stdout=False):

  """
  Run raytrace step for a single fid.
  """
  policy = ConfigParser.RawConfigParser()
  policy.read(imsim_config_file)
  assert policy.has_option('general', 'phosim_version')
  assert (version.LooseVersion(policy.get('general', 'phosim_version'))
          > version.LooseVersion('3.2.0'))
  fid = phosim.BuildFid(observation_id, cid, eid)
  ConfigureLogging(observation_id, fid, policy, log_to_stdout)
  logger.info('Running onechip with imsim_config_file=%s  fid=%s'
              ' filter_num=%s, instrument=%s run_e2adc=%s',
              imsim_config_file, fid, filter_num, instrument,
              run_e2adc)
  raytracer = PhosimManager.PhosimRaytracer(policy, observation_id, cid,
                                            eid, filter_num,
                                            instrument=instrument,
                                            run_e2adc=run_e2adc)
  return DoRaytrace(raytracer, pars_archive_name)


if __name__ == '__main__':

  usage = 'usage: %prog [options] imsim_config_file observation_id cid eid filter_num'
  parser = OptionParser(usage=usage)
  parser.add_option('-e', '--no_e2adc', dest='run_e2adc', action='store_false',
                    default=True, help='Do not run e2adc step.')
  parser.add_option('-i', '--instrument', dest='instrument', default='lsst')
  parser.add_option('-l', '--logtostdout', dest='log_to_stdout',
                    action='store_true', default=False,
                    help='Write logging output to stdout instead of log file.')
  parser.add_option('-p', '--pars_archive', dest='pars_archive_name',
                    default='pars.zip', help='Name of pars archive')
  (options, args) = parser.parse_args()
  if len(args) != 5:
    print 'Incorrect number of arguments.  Use -h or --help for help.'
    print usage
    quit()

  imsim_config_file = args[0]
  obsid = args[1]
  cid = args[2]
  eid = args[3]
  filter_num = args[4]
  sys.exit(main(imsim_config_file, obsid, cid, eid, filter_num,
                options.pars_archive_name, options.instrument, options.run_e2adc,
                options.log_to_stdout))
