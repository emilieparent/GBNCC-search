import sys
import os.path
import astropy.io.fits as pyfits
import psr_utils
from pyslalib import slalib
import numpy as np

if len(sys.argv) < 2:
  print "No filename supplied"
  sys.exit(1)

file_size = os.path.getsize(sys.argv[1])
fd = pyfits.open(sys.argv[1],"readonly",memmap=1)
basename=os.path.basename(sys.argv[1])
observation_name=basename[6:27]
sample_time=float(fd[1].header["TBIN"])
nsubint = int(fd[1].header["NAXIS2"])
samplespersubint=int(fd[1].header["NSBLK"])
observation_time=sample_time*nsubint*samplespersubint
source_name=fd[0].header["SRC_NAME"]
#start_utc
start_lst=float(fd[0].header["STT_LST"])
start_imjd=fd[0].header["STT_IMJD"]
start_smjd=fd[0].header["STT_SMJD"]
start_mjd=start_imjd+start_smjd/86400.0
project_id=fd[0].header["PROJID"]
observers=fd[0].header["OBSERVER"]
num_samples=nsubint*samplespersubint
ra_str=fd[0].header["RA"]
r,m,s = ra_str.split(":")
right_ascension=float(r)*10000.0+float(m)*100.0+float(s)
ra_deg=float(r)*15.0+float(m)/4.0+float(s)/240.0
dec_str=fd[0].header["DEC"]
d,m,s = dec_str.split(":")
if d[0:1] == "-":
  declination=float(d)*10000.0-float(m)*100.0-float(s)
else:
  declination=float(d)*10000.0+float(m)*100.0+float(s)
if d[0:1] == "-":
  dec_deg=float(d)-float(m)/60.0-float(s)/3600.0
else:
  dec_deg=float(d)+float(m)/60.0+float(s)/2600.0
galactic_longitude,galactic_latitude = slalib.sla_eqgal(ra_deg*np.pi/180.0,dec_deg*np.pi/180.0)
galactic_longitude *= 180.0/np.pi
galactic_latitude *= 180.0/np.pi
#os.system("koko J "+ra_str+" "+dec_str+" | tail -n 2 > /users/kstovall/GBNCC/kok#o.out")
#fin=open("/users/kstovall/GBNCC/koko.out","r")
#kokooutput=fin.readline()
#galactic_longitude=float(kokooutput[3:].strip())
#kokooutput=fin.readline()
#galactic_latitude=float(kokooutput[3:].strip())
obsType=fd[0].header["BACKEND"]
#fin.close()
#os.remove("/users/kstovall/GBNCC/koko.out")
#fd.close()
print "INSERT INTO headers (observation_name,sample_time,observation_time,source_name,start_lst,start_mjd,project_id,observers,file_size,num_samples,right_ascension,declination,galactic_longitude,galactic_latitude,ra_deg,dec_deg,obsType) VALUES ('%s','%.8f','%f','%s','%f','%f','%s','%s','%d','%d','%f','%f','%f','%f','%f','%f','%s')" % (observation_name,sample_time,observation_time,source_name,start_lst,start_mjd,project_id,observers,file_size,num_samples,right_ascension,declination,galactic_longitude,galactic_latitude,ra_deg,dec_deg,obsType)
