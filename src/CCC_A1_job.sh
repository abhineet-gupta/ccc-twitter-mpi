#!/bin/bash

# Partition for the job
#SBATCH -p physical

# The maximum running time of the job in days-hours:mins:sec
#SBATCH --time=00:02:00

# Maximum number of nodes, tasks/CPU cores and CPUs per task used by the job:
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1

# The name of the job:
#SBATCH --job-name=CCC_Twitter_AB

# Send yourself an email when the job:
# aborts abnormally (fails)
#SBATCH --mail-type=FAIL
#SBATCH --mail-type=END

# output from program goes into these files
#SBATCH -o 'output-%j.out'
#SBATCH -e 'error_output-%j.err'

# The modules to load:
module load Python/3.5.2-goolf-2015a

# The job command(s):
./CCC_A1_nonMPI.py

# sbatch [options] job
# showq -u
# scancel jobID
