#!/usr/bin/env python
import os, subprocess, shutil, glob, time, datetime, pytz, config, utils, database, pyslurm, time
import tarfile

to_move = glob.glob(config.baseoutdir+'/*/*/tobeviewed')
print "Will try to move results of %s jobs"%str(len(to_move))
k=0
for t in to_move:
	dirnm = t.split('/tobeviewed')[0]
	dir_subnm = dirnm.split('scratch/results/')[-1]
	newdir = os.path.join(config.baseoutdir_final,dir_subnm)
	files = glob.glob(dirnm+'/*')
	try:
		os.makedirs(newdir)
		os.chmod(newdir,0775)
		#Ensure all files are accessible by both aemcewen & rlynch
    		cmd = "setfacl -R -m u:aemcewen:rwx -m u:rlynch:rxw %s"%newdir
    		subprocess.call(cmd,shell=True)
	except:
		print " .. directory in project area exists. Will move results if empty "

	if len(glob.glob(newdir+'/*'))==0:
		print "\t Moving content of %s to %s"%(dirnm,newdir)
		for f in files:
			cmd = "rsync %s %s/"%(f,newdir)
			subprocess.call(cmd,shell=True)
			os.chmod(os.path.join(newdir,f),0664)
			os.remove(f)
		k+=1
	else: 
		print "\t Results already exists in %s, skipping..."%newdir

print "Moved %d job results"%k 
