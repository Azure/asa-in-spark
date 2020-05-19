#!/bin/bash
if [ "$1" == "debug" ]; then
  echo "[INFO] Not generating ASA-x.x.x.jar as debug mode is enabled and user wants to troubleshoot the build process"
  exit 0
fi

echo "Cleaning existing build files..."
rm -rf cpp/build dotnet/bin dotnet/obj java/target || true
echo "Older build deleted"

echo "Downloading maven dependencies..."
cd java 
mvn compile 
cd ..

echo "Creating cpp build..."
mkdir -p cpp/build
cd cpp/build
cmake ..
cmake --build .
cd ../..
echo "cpp build created successfully"


echo """
====================================================================================================================================
Make sure azure credentials were provided successfully while building the docker image. 
If not then stop this build.sh script and run
$ cd dotnet && dotnet restore --interactive
Open the URL on a browser and enter the code when prompted to authenticate

After the authentication, rerun the build.sh

For more information: 
Azure Credential provider was installed while building the docker image to interactively acquire credentials for Azure Artifacts
https://github.com/microsoft/artifacts-credprovider
====================================================================================================================================
"""

echo "Creating dotnet build ..."
# This is where it hits case sensitive dir name errors. Wierd, but it works if triggerred second time
cd dotnet && (dotnet build || dotnet build) && cd ..

echo "Creating ASA-x.x.x.jar ..."
cd java 
mvn package
# At this point jar should be created
ls -lh target/*.jar
echo "Jar created successfully"
cd ..

echo "Running sbt test..."
# Run test
cd scala && sbt test 
