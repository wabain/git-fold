#!/bin/bash

set -euxo pipefail

workdir=$(mktemp -d -t git-entropy-test)

echo "$workdir"
cd "$workdir"

git init

cat > test_file <<'EOF'
this is a file.
hello world
thsi is the end.
EOF

git add test_file
git commit -m "initial"

#
# A
#
git checkout -b A master

cat > test_file <<'EOF'
this is a file.
hello world
This is the end.
EOF

git commit -m "variant a" test_file

#
# B
#
git checkout -b B master

cat > test_file <<'EOF'
This is a file.
hello world
thsi is the end.
EOF

git commit -m "variant b" test_file

#
# Post merge
#
git checkout master
git merge --no-ff --no-edit A B

cat > test_file <<'EOF'
This is a file.
Hello world.
This is the end.
EOF

git add test_file

python3 -m git_entropy
