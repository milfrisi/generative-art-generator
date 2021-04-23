#!/bin/bash
# $ ./branch_create new_branch_name from_branch_name

git checkout $2
git checkout -b $1
git worktree add $1 $1
git checkout master
cd $1
git push --set-upstream origin $1

