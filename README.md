# I2R

## Set up
Use vscode devcontainer.
Run 'sh init.sh'.

## To do
Clean constants and DTOs (put them in a single file).

## Questions
Is it good practice to include the key and timestamp inside the event payload?
    It duplicates data but it is gonna have to be included anyway by flink to process it.