Project
Airline data ingestion using AWS services
Techstack - AWS cloud
Services used -  S3 , Event bridge rule , Glue etl , step function , SNS , Redshift and code build

Use case
As soon as the file arrives in the S3 bucket the Glue job execution starts and performs the incremental load of the fact table with the pre loaded 
Dimension tables in redshift and the notification of the job is send via SNS to subscribed email.

Data pipeline transformations:
The daily file will have the origination airport id  , destination airport id , departure delay and arrival delay.
The dimension table in redshift is already loaded with the airport referential information.
The ask is to load the fact table daily for the flights having delay of more than 60mins.

