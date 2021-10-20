# Electric Car Company Inventory Management
This application runs an inventory management system with a back-end based in Cloudera Operational Database

# Values are live and can be actively updated through the application
App will return immediate feedback if the requested quantity for the given part is more than is currently available

# Deployment Instructions
# Deploying in CML
1. Take the config.ini.template file and replace any variables wrapped in <>
    - Username/Password is your CDP username and workload password
    - Phoenix Thin Client URL can be found in the details of your operational database
    - Once values are replaced save file as `config.ini`
2. Install the dependencies
    - Go to sessions
    - Click "**New Session**"
    - Make sure to select the "**Python 3**" Kernel
    - Install the needed python packages by running
        `!pip3 install -r requirements.txt`
3. Test connection to Operational Database and Populate sample data
    - In the same session (or a new one) run `db_setup.py`
    - This should both test your connection to the operational database as well as load the necesary data
4. Go to the "**Applications**" tab in CML.
    - Click "**New Application**"
    - Give your applicaiton a unique name and subdomain. The subdomain is the most important and must be *unique* in this CML workspace.
    - "**Script**" will be `main.py`
    - "**Engine Kernel**" is "**Python 3**"
    - "**Engine Profile**" can be small, this is not an intense website
    - Click "**Create Application**"
5. The website is now deploying. This will take a few minutes. You can click the link on the application name in CML once the Status is "*Running*"