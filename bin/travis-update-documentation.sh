#! /usr/bin/env bash

# Copied from github.com/sympy/sympy
#
# This file automatically deploys changes to http://BayAreaMetro.github.io/fast-trips/.
# This will only happen when building on the master branch
#
# It requires an access token which should be present in .travis.yml file.
#
# Following is the procedure to get the access token:
#
# $ curl -X POST -u <github_username> -H "Content-Type: application/json" -d\
# "{\"scopes\":[\"public_repo\"],\"note\":\"token for pushing from travis\"}"\
# https://api.github.com/authorizations
#
# It'll give you a JSON response having a key called "token".
#
# $ gem install travis
# $ travis encrypt -r sympy/sympy GH_TOKEN=<token> env.global
#
# This will give you an access token("secure"). This helps in creating an
# environment variable named GH_TOKEN while building.
#
# Add this secure code to .travis.yml as described here http://docs.travis-ci.com/user/encryption-keys/

# Exit on error
set -e

echo "Building docs"
cd doc
doxygen doxygen.conf
sphinx-build -b html source build

cd ../../
echo "Setting git attributes"
git config --global user.email "easall@gmail.com"
git config --global user.name "Elizabeth Sall"

echo "Cloning repository"
git clone --quiet --single-branch --branch=gh-pages https://${GH_TOKEN}@github.com/BayAreaMetro/fast-trips.git  gh-pages > /dev/null 2>&1

cd gh-pages
rm -rf *
cp -R ../fast-trips/doc/build/** ./
touch .nojekyll
git add -A .

git commit -am "Update documentation after building $TRAVIS_BUILD_NUMBER"
echo "Pushing commit"
git push -fq origin gh-pages > /dev/null 2>&1
