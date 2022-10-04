# TDT4225

## How to run the MySQL database
Have been tested on Windows 11 and Kali linux
1. Install docker
2. Open a terminal in the project root folder (TDT4225)
3. Run `docker build -t mysql_db .`
4. Run `docker run -p 3306:3306 mysql_db` (optional add a `-d` tag if you dont want it to take up your terminal)

## Running the application
1. First you have to setup your python environment. The required packages are found in [requirements.txt](requirements.txt). Use the environment manager of your choice.
2. You first have to create the tables and insert the data, you do this by running [Task1.py](src/Task1.py). It will tell you in the console when it is complete
3. To run the queries simply run [Task2.py](src/Task2.py). You will get the result of the first query in the console, and be prompted to press enter to get the next query.

## Some more docker information
- To view all your containers you can run `docker container la -a` which will list both stopped and started containers.
- To stop a container simply run `docker stop {id}` where the id is the `CONTAINER ID` column from the previous command
- Start and remove is the same as stop just with `start` or `rm` instead of `stop`
- There is similar commands for the image you created with `docker build -t mysql_db .`. E.g. `docker image ls`