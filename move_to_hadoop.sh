#!/bin/bash

# Set variables
LOCAL_DIR="data/phnom_penh"                    # Local directory containing .xlsx files
CONTAINER_ID="namenode"                        # Docker container ID (NameNode)
CONTAINER_TMP_DIR="/tmp"                       # Temporary directory in the container
HDFS_DIR_INPUT="/user/hadoop/inputs"           # HDFS directory to store the input files
HDFS_DIR_MERGE="/user/hadoop/merged"           # HDFS directory to store the merged file
HDFS_DIR_CLEANED="/user/hadoop/cleaned"        # HDFS directory to store the cleaned file
HDFS_DIR_PARTITION="/user/hadoop/partitions"   # HDFS directory to store the partition files

# Function to create an HDFS directory if it doesn't exist
create_hdfs_dir() {
  local dir=$1
  echo "Checking if $dir exists in HDFS..."
  docker exec -it "$CONTAINER_ID" bash -c "hadoop fs -test -d $dir || hadoop fs -mkdir -p $dir"
}

# Function to copy .xlsx files to the container
copy_to_container() {
  echo "Copying .xlsx files to Docker container..."
  for file in "$LOCAL_DIR"/*.xlsx; do
    if [ -f "$file" ]; then
      echo "Copying $file to $CONTAINER_ID:$CONTAINER_TMP_DIR"
      docker cp "$file" "$CONTAINER_ID:$CONTAINER_TMP_DIR/"
    else
      echo "No .xlsx files found in $LOCAL_DIR."
    fi
  done
}

# Function to move files from container to HDFS
move_to_hdfs() {
  local hdfs_dir=$1
  echo "Moving files from Docker container to HDFS directory $hdfs_dir..."
  docker exec -it "$CONTAINER_ID" bash -c "hadoop fs -put $CONTAINER_TMP_DIR/*.xlsx $hdfs_dir/"
}

# Function to clean up the container temporary directory
clean_container_tmp() {
  echo "Cleaning up files from $CONTAINER_TMP_DIR in Docker container..."
  docker exec -it "$CONTAINER_ID" bash -c "rm $CONTAINER_TMP_DIR/*.xlsx"
}

# Function to verify files in HDFS
verify_hdfs() {
  local hdfs_dir=$1
  echo "Verifying files in HDFS directory $hdfs_dir..."
  docker exec -it "$CONTAINER_ID" bash -c "hadoop fs -ls $hdfs_dir"
}

# Main Script Execution
echo "Starting the data processing pipeline..."

# Step 1: Copy .xlsx files from local machine to Docker container
copy_to_container

# Step 2: Create necessary HDFS directories
create_hdfs_dir "$HDFS_DIR_INPUT"
create_hdfs_dir "$HDFS_DIR_MERGE"
create_hdfs_dir "$HDFS_DIR_CLEANED"
create_hdfs_dir "$HDFS_DIR_PARTITION"

# Step 3: Move files from /tmp/ inside the container to HDFS
move_to_hdfs "$HDFS_DIR_INPUT"

# Step 4: Clean up the /tmp directory in the Docker container
clean_container_tmp

# Step 5: Verify the files are in HDFS
verify_hdfs "$HDFS_DIR_INPUT"

echo "Process complete."

# docker exec -it namenode bash -c "hadoop fs -rm -r /user/hadoop/inputs/*"