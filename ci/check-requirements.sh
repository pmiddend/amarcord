#!/usr/bin/env bash

set -eu
set -o pipefail

# The way this is done here with the -old.txt suffix is important. "uv pip compile" considers
# the existing compile output "requirements.txt". It only adds new stuff, and doesn't update
# any dependenciecs. So we have to copy the old file and compile the new one and then compare.
#
# Previously, we compiled into a new thing and then compared with the old one. That doesn't
# work for the check. We do that in the regenerate code, however, since then we can
# deliberately update.
cp requirements.txt requirements-old.txt
uv pip compile pyproject.toml -o requirements.txt
# Thanks https://unix.stackexchange.com/questions/17040/how-to-diff-files-ignoring-comments-lines-starting-with
diff -u -B <(grep -vE '^\s*(#|$)' requirements-old.txt)  <(grep -vE '^\s*(#|$)' requirements.txt)
