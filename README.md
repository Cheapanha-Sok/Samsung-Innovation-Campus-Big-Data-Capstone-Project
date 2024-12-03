# Set Up Python Environment
### Follow the steps below to set up the Python environment in your local project.
#### 1. Install Python in Your Local Project
Ensure that Python is configured on your local machine. To verify,run: 
````courseignore
python3 --version or python --version
````
If Python is not installed, you can install it and set up a virtual environment:
````courseignore
python3 or python -m venv /path/to/env
````
#### 2. Activate the Environment
Activate the virtual environment by running:
````courseignore
source /path/to/env/bin/activate
````
#### 3. Install Necessary Modules
After activating the environment, install the required modules using:
````courseignore
pip install -r requirements.txt
````


# Move Files to Hadoop HDFS via Docker
### Follow the steps below to move a file from your local system to Hadoop's HDFS using Docker.

#### 1. Make the Script Executable
First, ensure that the script has the appropriate permissions to execute. Run the following command to make the script executable: 
````courseignore
chmod +x move_to_hadoop.sh
````

#### 2. Run the Script
Once the script is executable, you can run it from the terminal. Execute the following command to initiate the process:
````courseignore
./move_to_hadoop.sh
````

