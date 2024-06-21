#!/bin/bash

GIT=$(git rev-parse --git-dir)
ln -s ../../scripts/pre_commit.sh $GIT/hooks/pre-commit 
