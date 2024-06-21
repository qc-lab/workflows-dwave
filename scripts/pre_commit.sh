#!/bin/bash

error=0

function run_linter() {
    echo -ne "\033[1mRunning $2 ...\033[0m"
    output=$($1 2>&1) 
    result=$?

    if [ $result -eq 0 ]; then
        echo -e "\033[1K\r\033[1mRunning $2 ✅\033[0m"
    else
        echo -e "\033[1K\r\033[1mRunning $2 ❌\033[0m"
        echo -e "\n$output"
    fi

    ((error+=result))
}

run_linter "mypy ." "Running Mypy"
run_linter "black --diff ." "Running Black"
run_linter "isort --check-only --diff ." "Running ISort"

exit $error
