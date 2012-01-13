--------------------------
Python ImSim Control Files
--------------------------

These files are for use with the LSST Image Simulator (ImSim).
Currently, in order to use them to run ImSim, they must be copied into
the root of the ImSim source tree.  For revisions predating and
including 25503, these files are part of the ImSim tree itself (after
that, they were moved over here).  This means that in order to run
with an earlier revision, you will have to overwrite the existing versions of
these files.

The correct procedure is to:
  1) Check out proper revision of ImSim (see below)
  2) Check out a compatible revision of the Python control package
  3) Copy the Python control files into the ImSim source tree.

Eventually, it will be the case that the files in this package will not
need to be in the ImSim source tree.

---------
Revisions
---------

The following tags of the Python control package work with the
following revisions of ImSim:

Tag	  ImSim Rev    ImSim Tag  Notes:
------	  ---------    ---------  --------------------------------------------
v-1.0	  23580	       none	  Compatable w/ Nicole's documented version
				  except that it uses .cfg file and not .paf


