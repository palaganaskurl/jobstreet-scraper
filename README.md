# JobStreet Scraper
Scraper for https://www.jobstreet.com.ph using their initially rendered HTML and GraphQL endpoints.
  
## How to use
 - Install requirements
    - `pip install -r requirements.txt`
 - Edit config
     - Open `jobs_and_skills.py` 
     - The config file has the structure of "Job Name": ["Skill1", "Skill2"]
     - Job Name is the job to be searched in JobStreet and the equivalent values are the skills to be extracted from the specific job.
 - Run `main.py`
     - `python main.py` 

## Data
 - Data extracted only consists of Job title, Company Name, Job Description, Years Required, Keywords Extracted in the Job Description, Job Page URL, and Job ID. 
 - Feel free to add whatever you want to use from their GraphQL API endpoint return.
