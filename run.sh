
#Enstead of using a separate run.sh file, the python file itself is made a self executing .py shellscript and runs as ./combined_program.py..
#Before running the ./combined_program.py the user needs to have installed the python libraries of upgraded v1.7+ of numpy, pandas and hashlib
#If any of these libraries are not installed please do it using the below sequence of commands.
#This file acts like a setup.py file which builds all dependencies required for program execution.

## NOTE: This script and installation steps are developed and recommended using Ubuntu Linux OS platform
#Uncomment to execute whichever required

#To install pip from source
#sudo apt-get install python-pip python-dev build-essential 
#sudo pip install --upgrade pip 
#sudo pip install --upgrade virtualenv 

#To install pandas first upgrade numpy and then install pandas using the following commands
#sudo pip install numpy --upgrade
#sudo pip install pandas

#To install hashlib library execute following command
#sudo pip install hashlib



