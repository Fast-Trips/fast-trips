#! /usr/bin/env bash

# This file automatically deploys changes to http://BayAreaMetro.github.io/fast-trips/.
# This will give you an access token("secure"). This helps in creating an
# environment variable named GH_TOKEN while building.
#
# Add this secure code to .travis.yml as described here http://docs.travis-ci.com/user/encryption-keys/

# Exit on error
set -e

ft_dir="$PWD"

echo "Building docs"
cd doc
doxygen doxygen.conf
sphinx-build -b html source build

echo "Creating temporary directory"
tmpdir = $("ft_ghpages" $(mktemp -u))
cd tempdir
echo "Cloning repository"

git clone --quiet --single-branch --branch=gh-pages git@github.com:BayAreaMetro/fast-trips.git gh-pages > /dev/null 2>&1

cd gh-pages
rm -rf *

cp -R ../fast-trips/doc/build/** ./
touch .nojekyll
##git add -A .

##git commit -am "Update documentation"
##echo "Pushing commit"
##git push -fq origin gh-pages > /dev/null 2>&1
