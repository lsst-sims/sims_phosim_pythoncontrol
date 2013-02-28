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

logger = logging.getLogger(__name__)


def ConfigureLogging(observation_id, fid, policy, log_to_stdout):
  if log_to_stdout:
    log_fn = None
  else:
    if policy.has_option('general', 'log_dir'):
      # Log to file in log_dir
      obsid = PhosimManager.ObservationIdFromTrimfile(
        trimfile, extra_commands=options.extra_commands)
      log_dir = policy.get('general', 'log_dir')
      if not os.path.exists(log_dir):
        os.makedirs(log_dir)
      log_fn = os.path.join(log_dir, 'fullFocalplane_%s.log' % obsid)
    else:
      log_fn = '/tmp/fullFocalplane.log'
  log_format = '%(asctime)s %(levelname)s:%(name)s:  %(message)s'
  log_level = logging.DEBUG if policy.getint('general', 'debug_level') else logging.INFO
  logging.basicConfig(filename=log_fn, filemode='w', level=log_level, format=log_format)


def main(imsim_config_file, observation_id, cid, eid, filter_num,
         instrument='lsst', run_e2adc=True, log_to_stdout=False);

  """
  Run raytrace step for a single fid.
  """
  policy = ConfigParser.RawConfigParser()
  policy.read(imsim_config_file)
  assert policy.has_option('general', 'phosim_version')
  assert (version.LooseVersion(policy.get('general', 'phosim_version'))
          > version.LooseVersion('3.2.0'))
  ConfigureLogging(observation_id, policy, log_to_stdout)
  raytracer = PhosimManager.PhosimRaytracer(policy, observation_id, cid,
                                            eid, filter_num,
                                            instrument=instrument,
                                            run_e2adc=run_e2adc)


    
    phosim_version = policy.get('general', 'phosim_version')
  else:
    phosim_version = '3.0.1'
  ConfigureLogging(trimfile, policy, log_to_stdout)
  # print 'Running fullFocalPlane on: ', trimfile
  logger.info('Running fullFocalPlane on: %s ', trimfile)

  # print 'Using Imsim/Phosim version', phosim_version
  logger.info('Using Imsim/Phosim version %s', phosim_version)
  scheduler = policy.get('general','scheduler2')
  if version.LooseVersion(phosim_version) < version.LooseVersion('3.1.0'):
    if len(sensor_ids.split('|')) > 1:
      logger.critical('Multiple sensors not supported in version < 3.1.0.')
      return 1
    sensor_id = '' if sensor_ids == 'all' else sensor_ids
    return DoPreprocOldVersion(trimfile, policy, extra_commandsm,scheduler,
                               sensor_id)
  elif 
    if sensor_ids != 'all':
      logger.critical('Single exposure mode is currently not supported for'
                       ' phosim > 3.2.0')
      return 1
    return DoPreproc(trimfile, imsim_config_file, extra_commands, scheduler,
                     skip_atmoscreens=skip_atmoscreens,
                     keep_scratch_dirs=keep_scratch_dirs)
  logger.critical('Unsupported phosim version %s', phosim_version)
  return 1

if __name__ == '__main__':

  usage = 'usage: %prog [options] imsim_config_file observation_id cid eid filter_num'
  parser = OptionParser(usage=usage)
  parser.add_option('-e', '--no_e2adc', dest='run_e2adc', action='store_false',
                    default=True, help='Do not run e2adc step.')
  parser.add_option('-i', '--instrument', dest='instrument', default='lsst')
  parser.add_option('-l', '--logtostdout', dest='log_to_stdout',
                    action='store_true', default=False,
                    help='Write logging output to stdout instead of log file.')
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
  sys.exit(main(obsid, cid, eid, filter_num, options.instrument,
                options.run_e2adc, options.log_to_stdout))
