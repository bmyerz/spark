#!/bin/bash
#SBATCH --partition=pal
#SBATCH --time=120
#SBATCH --job-name=gups
#SBATCH --exclusive
#SBATCH --ntasks-per-node=16

# create hosts file
GUPS_HOSTS=~/escience/spark/hosts
scontrol show hostname $SLURM_NODELIST | awk '{ print $1 "-ib" }' >$GUPS_HOSTS
#scontrol show hostname $SLURM_NODELIST  >$GUPS_HOSTS

ppn=16

srun --ntasks-per-node=$ppn -- ./rungups-pal.sh $GUPS_HOSTS
