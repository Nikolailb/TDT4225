# TDT4225

## Structure
The dataset is located in the resources folder: [dataset](resources/dataset/)
Otherwise I used the resources folder to store copies of the cleaned data as CSV files for faster reads when testing inserts.
All code is in the [src](src/) folder:
- [DbConnector.py](src/DbConnector.py) and [example.py](src/example.py) are leftovers from skeleton code. Left them in since they are nice to have for testing if the MongoDB db is running as it should.
- [DataHandler.py](src/DataHandler.py): Reads the data and converts it into pandas dataframes after cleaning.
- [DbController.py](src/DbController.py): Connects to the DB and handles all further queries. This includes creation of tables and insertion.
- [Task1.py](src/Task1.py): The code for completing task 1. Mostly uses a combination of [DataHandler.py](src/DataHandler.py) and [DbController.py](src/DbController.py) to insert data into database.
- [Task2.py](src/Task2.py): Runs the queries specified in Task2. Uses an instance of [DbController.py](src/DbController.py) to run the queries.

## How to run the MongoDB database
Have been tested on Windows 11 and Kali linux
1. Install docker
2. Open a terminal in the project root folder (TDT4225)
3. Run `docker build -t mongodb .`
4. Run `docker run -p 27017:27017 mongodb`
5. After the terminal stops updating you can proceed to start running the application.

## Running the application
1. First you have to setup your python environment. The required packages are found in [requirements.txt](requirements.txt). Use the environment manager of your choice.
2. You first have to create the tables and insert the data, you do this by running [Task1.py](src/Task1.py). It will tell you in the console when it is complete
3. To run the queries simply run [Task2.py](src/Task2.py). You will get the result of the first query in the console, and be prompted to press enter to get the next query.

## Some more docker information
- To view all your containers you can run `docker container la -a` which will list both stopped and started containers.
- To stop a container simply run `docker stop {id}` where the id is the `CONTAINER ID` column from the previous command
- Start and remove is the same as stop just with `start` or `rm` instead of `stop`
- There is similar commands for the image you created with `docker build -t mongodb .`. E.g. `docker image ls`