# cs435_Final_Project

data path = ~cs435/cs435a

python kernal = /usr/bin/python3.9

additions to .bashrc:

    source /etc/profile.d/modules.sh module 
    module purge
    module load courses/cs435/pa3
    module load python/bundle-3.9
    module load cuda/11.2

    export PYSPARK_PYTHON="/usr/local/python-env/py39/bin/python3"
    export PYSPARK_DRIVER_PYTHON="/usr/local/python-env/py39/bin/python3"
    export CUDA_VISIBLE_DEVICES=0

required libraries (that aren't included in modules):
    # pip3 install pyspark --user
    # pip3 install tensorflow --user
    # pip3 install seaborn --user