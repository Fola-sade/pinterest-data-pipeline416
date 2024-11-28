# Milestone 9

- ## Create data streams using Kinesis Data Streams

In AWS navigate to the Kinesis console and follow, Amazon Kineis > Data streams > select 'Create data stream' button.


Choose the desired name for your stream i.e. streaming-<your_UserId>-pin, and input this in the 'Data stream name' field. 

The 'On-demand' is selected from the 'Capacity mode' and click 'Create data stream'. When your stream is finished creating the status will change from 'Creating' to 'Active'.

This process is repeated to create the following three streams:

- streaming-<your_UserId>-pin
- streaming-<your_UserId>-geo
- streaming-<your_UserId>-user

- ## Configure an API with Kinesis proxy integration

Configure the previously created REST API that was built for the 'Kafka REST proxy integration' to allow it to invoke Kinesis actions. The AWS account provided had been granted the necessary permissions to invoke Kinesis actions, so it was not required to create an IAM role for the API to access Kinesis.

The access role had the following structure: <your_UserId-kinesis-access-role>. The ARN of this role was copied from the IAM console, under Roles. This is used when setting up the Execution role for the integration point of all the methods created.

The API should be able will invoke the following actions:

- List streams in Kinesis
- Create, describe and delete streams in Kinesis
- Add records to streams in Kinesis

- ## List 'streams' in Kinesis

In AWS via the `API Gateway` console:

- Select the desired API that was created before i.e. <your_UserId> and navigate to the 'Resources' tab.
- Click 'Create resource' button to start provisioning a new resource.
- Under Resource name', type 'streams' and leave the rest as default and click the 'Create resource' button.

- ## Creating a 'GET' method for 'streams' resource

From the 'API Gateway' console, select the 'API' followed by the 'Resources' tab:  

- Select the created 'streams' resource, and 'Create method' button. 


- Under 'Method details':
    - Select 'GET' as the 'method type'
    - 'Integration' as 'AWS Service'
    - 'AWS Region' choose 'us'-east-1'
    - 'AWS Service' select 'Kinesis'
    - 'HTTP method' select 'POST' (as we will have to invoke Kinesis's ListStreams action)
    - 'Action Type' select 'Use action name'
    - 'Action name' type `ListStreams`
    - Execution role you should copy the `ARN` of your Kinesis Access Role (created in the previous section)
    - Click 'Create method' to finalise provisioning of this method

- ## Configuring the 'GET' method for 'streams' resource

- After carrying out the above you are redirected to the 'Method Execution' page, otherwise from the 'API Gateway' console, select the desired 'API' followed by the 'Resources' tab:

    - Select the 'GET' resource and click the 'Integration request' tab followed by the 'Edit' button
    - Expand the 'URL request headers parameters' panel
    - Under 'Name' type `Content-Type`
    - Under 'Mapped from' type `'application/x-amz-json-1.1'`


    - Expand the 'Mapping templates' panel
    - Choose 'Add mapping template' button
    - Under 'Content type' type `application/json`
    - Under 'Template body' type `{}` in the template editor
    - Click the 'Save' button



- ## Create, describe and delete streams in Kinesis

- From the 'API Gateway' console, select the 'API' followed by the 'Resources' tab:
    - Select 'streams' under 'Resources' to start creating a new `child resource` 
    - Select 'Create resource' and name the 'Resource name' as `{stream-name}` 
    - Leave the rest as default and select the 'Create resource' button


- ## Creating methods POST, GET and DELETE for the child resource

- The following three Methods for `/{stream-name}` resource is then created and configured: POST, GET and DELETE.
- Note: When creating the `GET`, `POST` and `DELETE` methods. Repeat the steps from before when creating and configuring of the `GET` method. Remember to change their respective 'Action name' and 'Template body' accordingly as below.

    - Select the `/{stream-name}` resource and click 'Create method' followed by selecting desired 'Method type'

        - `GET` method: 
            - 'Action name', type `DescribeStream`
            - 'Template body' type the following into the template editor:

                ```python
                {
                    "StreamName": "$input.params('stream-name')"
                }
                ```

        - `POST` method: 
            - 'Action name', type `CreateStream`
            - 'Template body' type the following into the template editor:

                ```python
                {
                    "ShardCount": #if($input.path('$.ShardCount') == '') 5 #else $input.path('$.ShardCount') #end,
                    "StreamName": "$input.params('stream-name')"
                }
                ```

        - `DELETE` method: 
            - 'Action name', type `DeleteStream`
            - 'Template body' type the following into the template editor:

                ```python
                {
                    "StreamName": "$input.params('stream-name')"
                }
                ```
            

- ## Add records to streams in Kinesis

- From the `/{stream-name}` resource, two new resources `/record` and `/records` are created with `PUT` method. This is done in the same way as before but with the their respective 'Action name' and 'Template body' details (see below).

    - Select the `/{stream-name}/record` resource and click 'Create method' followed by selecting desired 'Method type'

        - `PUT` method: 
            - 'Action name', type `PutRecord`
            - 'Template body' type the following into the template editor:

                ```python
                {
                    "StreamName": "$input.params('stream-name')",
                    "Data": "$util.base64Encode($input.json('$.Data'))",
                    "PartitionKey": "$input.path('$.PartitionKey')"
                }
                ```

    - Select the `/{stream-name}/records` resource and click 'Create method' followed by selecting desired 'Method type'

        - `PUT` method: 
            - 'Action name', type `PutRecords`
            - 'Template body' type the following into the template editor:

                ```python
                {
                    "StreamName": "$input.params('stream-name')",
                    "Records": [
                    #foreach($elem in $input.path('$.records'))
                        {
                            "Data": "$util.base64Encode($elem.data)",
                            "PartitionKey": "$elem.partition-key"
                        }#if($foreach.hasNext),#end
                        #end
                    ]
                }
                ```


- ## Data Streaming - Step 1 of 3 [Send data to the Kinesis streams]

A new script `user_posting_emulation_streaming.py` is created, that builds upon the initial `user_posting_emulation.py` previously worked on.

This script sends requests to the API, which adds one record at a time to the streams created. Data is sent from the three Pinterest tables to their corresponding Kinesis stream until interrupted.

- Open a terminal on the local machine where the 'project directory' is located
- Run the script:

```bash
python user_posting_emulation_streaming.py
```

- ## Data Streaming - Step 2 of 3 [Visualise data coming into Kinesis Data Streams]

Once data has been sent to Kinesis Data Stream (as above), a successful 200 response.status_code is seen. This data is visualised in the Kinesis console on AWS.

- Navigate to the `Kinesis` console and select the 'stream' you want to look at
- Choose the 'Data viewer' section
- Select the 'Shard' (data will normally be stored in the first shard shardId-000000000000 but check others too).
- 'Starting position' select 'At timestamp'. 
- 'Start date', corresponds to the date at which you send data to your stream 
- 'Start time', the time (approximation) at which you start sending data
- Press 'Get records' to visualise the data that has been send to the stream

- ## Data Streaming - Step 3 of 3 [Read, transform and write data from Kinesis streams in Databricks]

- A new Notebook is created in Databricks ([streaming_data_processing to Kinesis](<streaming_data_processing.py>)) and credentials read in from the `Delta table`, located at `dbfs:/user/hive/warehouse/authentication_credentials`, to retrieve the `Access Key ID` and `Secret access key`. The same steps from earlier are repated as followed for the batch data.
    - Ingest data into Kinesis Data Streams and verify data reception in the Kinesis console (such as sending data to an API with a Kinesis proxy integration - (Data Streaming - Step 1 and 2)).
    - Run the Databricks Notebook ([streaming_data_processing to Kinesis](<streaming_data_processing.py>)) and read data from the three created streams 
- Transform Data from Kinesis Streams in Databricks:
    - Define schema for stream tables
    - Deserialise streamed data
    - Clean the streaming data similar to the batch data cleaning process
- Write cleaned streamed data to `Delta tables`
     - Save as: <your_UserId>_pin_table, <your_UserId>_geo_table, and <your_UserId>_user_table










Task 5: Transforms orm Kinesis streams in Databricks

Clean the streaming data in the same way you have previously cleaned the batch data.

Task 6: Write the streaming data to Delta Tables

Once the streaming data has been cleaned, you should save each stream in a Delta Table. You should save the following tables: <your_UserId>_pin_table, <your_UserId>_geo_table and <your_UserId>_user_table.

Task 7: Document your exp

Task 8: GitHub

Save the code you have created in Databricks to your local project repository.


Update your GitHub repository with the latest code changes from your local project. Start by staging your modifications and creating a commit. Then, push the changes to your GitHub repository.

Finally, you can upload the diagram of the architecture you created using this template .
