#!/bin/bash

# Print a message
echo "Hello from the dummy script!"

# Create a temporary file
TEMP_FILE="/tmp/dummy_file.txt"
echo "This is a dummy file created by the script." > $TEMP_FILE

# List the contents of the /tmp directory
echo "Listing contents of /tmp:"
ls -l /tmp

# Print the content of the created file
echo "Content of $TEMP_FILE:"
cat $TEMP_FILE

# Exit successfully
exit 0

