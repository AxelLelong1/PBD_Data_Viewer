# PBD_Data_Viewer

## Setup
### Dates

DATA ARE FETCHED FROM 2020 TO 2021 BY DEFAULT
If you want to change that range, go to `etl/etl.py` in the main function at the end of the file
You'll see two `storefile` function with dates, change them to you liking

### Docker
Change the database volume in the `dockercompose.yml` in the `etl` container
replace `/home/owen/bourse/data` by the folder containing data in YOUR computer


## Lauching
Be in the docker folder in run `make all`
BOURSORAMA DATA TAKE TIME TO BE INDEXED, you'll see data in the dashboard (`localhost:8050`) arriving everytime you reload the page.