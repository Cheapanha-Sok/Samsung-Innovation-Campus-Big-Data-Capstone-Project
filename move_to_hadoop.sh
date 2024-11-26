#!/bin/bash

# Set variables
LOCAL_DIR="data/phnom_penh"        # Local directory containing .xlsx files
CONTAINER_ID="namenode"        # Docker container ID (NameNode)
CONTAINER_TMP_DIR="/tmp"           # Temporary directory in the container
HDFS_DIR="/user/hadoop/input"      # HDFS directory to store the files

# Step 1: Copy .xlsx files from local machine to Docker container
echo "Copying .xlsx files to Docker container..."
for file in $LOCAL_DIR/*.xlsx; do
  if [ -f "$file" ]; then
    echo "Copying $file to $CONTAINER_ID:$CONTAINER_TMP_DIR"
    docker cp "$file" "$CONTAINER_ID:$CONTAINER_TMP_DIR/"
  fi
done

# Step 2: Create the HDFS directory if it doesn't exist
echo "Checking if $HDFS_DIR exists in HDFS..."
docker exec -it $CONTAINER_ID bash -c "hadoop fs -test -d $HDFS_DIR || hadoop fs -mkdir -p $HDFS_DIR"

# Step 3: Move files from /tmp/ inside the container to HDFS
echo "Moving files from Docker container to HDFS..."
docker exec -it $CONTAINER_ID bash -c "hadoop fs -put $CONTAINER_TMP_DIR/*.xlsx $HDFS_DIR/"

# Step 4: (Optional) Clean up the /tmp directory in the Docker container
echo "Cleaning up files from $CONTAINER_TMP_DIR in Docker container..."
docker exec -it $CONTAINER_ID bash -c "rm $CONTAINER_TMP_DIR/*.xlsx"

# Step 5: Verify the files are in HDFS
echo "Verifying files in HDFS..."
docker exec -it $CONTAINER_ID bash -c "hadoop fs -ls $HDFS_DIR/"

echo "Process complete."
