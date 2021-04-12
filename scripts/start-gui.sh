#!/usr/bin/env bash

BASE_PATH="/gpfs/cfel/cxi/common/public/development/AMARCORD"

__conda_setup="$('/software/anaconda3/5.2/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/software/anaconda3/5.2/etc/profile.d/conda.sh" ]; then
        . "/software/anaconda3/5.2/etc/profile.d/conda.sh"
    else
        export PATH="/software/anaconda3/5.2/bin:$PATH"
    fi
fi
unset __conda_setup

conda run --prefix "$BASE_PATH/gui-env" --cwd "$BASE_PATH/amarcord-xfel-unstable" python -m amarcord.xfel_gui
