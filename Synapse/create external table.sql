create database SCOPED CREDENTIAL cred_dev
with 
    IDENTITY ='Managed Identity'



create EXTERNAL data SOURCE source_silver
with
(
    LOCATION = 'https://azuredl2.blob.core.windows.net/silver',
    CREDENTIAL = cred_dev
)


create EXTERNAL data SOURCE source_gold
with
(
    LOCATION = 'https://azuredl2.blob.core.windows.net/gold',
    CREDENTIAL = cred_dev
)