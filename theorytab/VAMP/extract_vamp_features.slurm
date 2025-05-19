#!/bin/bash

#SBATCH --nodes 1
#SBATCH --qos=cpu
#SBATCH --mem=24G
#SBATCH --cpus-per-task=12
#SBATCH --partition=p_general
#SBATCH --job-name vamp
#SBATCH --output log.out

# ------------- NAO EXCLUIR AS LINHAS ABAIXO -----------------
port=$(($SLURM_JOBID % 1000 + 9000))
node="$(hostname -s).sense.dcc.ufmg.br"
user=$(whoami)

echo ""
echo "[${user}@slurm] Rodando em: ${node}:${port}"
echo ""

# ---------- INSIRA MODULOS E SEU ENVIRONMENT AQUI -----------

source activate harmony
conda activate harmony

echo "Available environments:"
conda env list

# ---------------------- NAO EXCLUIR -------------------------

python extract_vamp_features.py
