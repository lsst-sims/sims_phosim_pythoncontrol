--------------------------
Python ImSim Control Files
--------------------------

These files are for use with the LSST Image Simulator (ImSim).  As of
tag v-1.1, the Python scripts no longer need to be in the ImSim source
tree when they are executed.

For ImSim revisions predating and including 25503, these Python
control scripts are still part of the ImSim tree itself (after that,
they were moved over here).  The v-1.1 Python scripts and above will
ignore any Python scripts in the ImSim root, so you should not have to
worry about deleting them.

The correct procedure is to:
  1) Check out proper revision of ImSim (see below)
  2) Check out a compatible revision of the Python control package
  3) Execute from the directory containing the Python control package


---------
Revisions
---------

The following tags of the Python control package work with the
following revisions of ImSim:

Tag	  ImSim Rev    ImSim Tag  Notes:
------	  ---------    ---------  --------------------------------------------
v-1.0	  23580	       none	  Compatable w/ Nicole's documented version
				  except that it uses .cfg file and
				  not .paf.  In this version, the
				  Python control files must be run
				  from the root of the ImSim source tree.


v-1.1     25315        v-2.2.1    Still compatable with Nicole's documented
				  version (except for using .cfg files)
				  but works with ImSim tag v-2.2.1 *and*
				  Python files can reside in a separate 
				  directory.

v-2.0     25583        v-2.2.1    Major rewrite.  No longer compatable
                                  with Nicole's version.  Reorg of
                                  config params to better accomodate
                                  Exacycle.

v-2.1                  v-2.2.1    Reorganization in the handling of
                mirrors/exacycle  the shared data (QE, height maps, SEDs)
		                  to be more flexible and accomodate
				  shared storage architectures.

	
