#!/bin/bash
#
# Setzt die neue Version der Release
# 

# Festlegen, welcher teil erhöht werden soll
#-------------------------------------------
if [ "$1" == "" ]; then
	range="patch"
else
	range="$1"
fi

# Build und Version auslesen und neuen Versionsstring zusammensetzen
#-------------------------------------------------------------------
build=$(bump2version --allow-dirty --dry-run --list release | grep new_version | sed -r s,"^.*-",,)
version=$(bump2version --allow-dirty --dry-run --list $range | grep new_version | sed -r s,"^.*=",, | sed -r s,"\-.*$",,)
new_version="$version-$build"

# Nun Version setzen
#-------------------
bump2version --allow-dirty --new_version $new_version 






#<----------------- .git/hook/pre-commit script START ----------------->

##!/bin/sh
##
## Passt die Buildnummer vor dem Commit an
## 
## 
## 
##
## 
#
#cd ~/Scripts/imports/brokerapi
#bump2version --no-commit --no-tag --allow-dirty build

#<----------------- .git/hook/pre-commit script END ----------------->
