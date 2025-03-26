#!/bin/bash
#SBATCH --job-name=parallel_code
#SBATCH --output=output_%j.log
#SBATCH --ntasks=16
#SBATCH --time=02:00:00
#SBATCH --mem=4GB
#SBATCH --partition=standard
#SBATCH --cpus-per-task=1

module load python/3.10.12

python3 big_data_1.py
