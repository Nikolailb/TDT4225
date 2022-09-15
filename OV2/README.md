# TDT4225

## How to run
1. Install docker
2. Open a terminal in the project root folder (TDT4225)
3. Run `docker build -t mysql_db .`
4. Run `docker run -p 3306:3306 mysql_db` (optional add a `-d` tag if you dont want it to take up your terminal)