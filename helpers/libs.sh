#!/bin/sh

# Get the Python version
version=$(python --version 2>&1)
python_version=$(echo "$version" | cut -d ' ' -f 2 | cut -d '.' -f 1,2)

# Construct the jar path
jar_path=./.venv/lib/python$python_version/site-packages/pyspark/jars/

# Check if the jar path exists, create it if it doesn't
if [ ! -d "$jar_path" ]; then
    mkdir -p "$jar_path" || { echo "Failed to create jar path directory"; exit 1; }
fi

# Define the jar files and their URLs
jars_and_urls="hadoop-aws-3.3.1.jar|https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar
aws-java-sdk-bundle-1.11.901.jar|https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar
postgresql-42.2.18.jar|https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.18/postgresql-42.2.18.jar"

# Loop through the jar files and URLs
echo "$jars_and_urls" | while IFS="|" read -r jar url; do
    echo "Processing $jar..."
    if [ -f "$jar_path/$jar" ]; then
        echo "File $jar already exists in $jar_path, skipping download."
    else
        echo "Downloading $jar from $url..."
        wget -q --timeout=30 --tries=3 "$url" -O "$jar"
        if [ $? -eq 0 ]; then
            echo "Downloaded $jar."
            mv "$jar" "$jar_path" && echo "Moved $jar to $jar_path."
        else
            echo "Failed to download $jar."
        fi
    fi
done

echo "Script completed."
